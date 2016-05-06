package distil

import (
	"fmt"
	"strings"

	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
	"github.com/pborman/uuid"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const CHANGED_RANGE_RES uint8 = 38

type Stream struct {
	ds   *DISTIL
	id   uuid.UUID
	path string
}

func findAssertOne(col *mgo.Collection, key string, value string) bson.M {
	var q *mgo.Query = col.Find(bson.M{key: value})
	c, err := q.Count()
	if err != nil {
		panic(err)
	} else if c == 0 {
		return nil
	} else if c != 1 {
		panic(fmt.Sprintf("Multiple streams with %s = %s", key, value))
	}

	var result bson.M
	err = q.One(&result)
	if err != nil {
		panic(err)
	}

	return result
}

func (ds *DISTIL) StreamFromUUID(id uuid.UUID) *Stream {
	//return nil if it doesn't exist
	var result bson.M = findAssertOne(ds.col, "uuid", id.String())

	if result == nil {
		return nil
	}

	pathint, ok := result["Path"]
	if !ok {
		panic(fmt.Sprintf("Document for UUID %s is missing required field 'Path'", id.String()))
	}

	path, ok := pathint.(string)
	if !ok {
		panic(fmt.Sprintf("Value of Path for stream with UUID %s is not a string", id.String()))
	}

	return &Stream{ds: ds, id: id, path: path}
}

func (ds *DISTIL) ListUpmuPaths() []string {
	q := ds.col.Find(bson.M{"Metadata.SourceName": "uPMU", "Path": bson.M{"$regex": ".*L1MAG"}})
	rv := []string{}
	iter := q.Iter()
	var ob struct {
		Path string `bson:"Path"`
	}
	for iter.Next(&ob) {
		rv = append(rv, strings.TrimSuffix(ob.Path, "/L1MAG"))
	}
	return rv
}

func (ds *DISTIL) StreamsFromUUIDs(ids []uuid.UUID) []*Stream {
	//loop over above
	var streams = make([]*Stream, len(ids))
	for i, id := range ids {
		streams[i] = ds.StreamFromUUID(id)
	}
	return streams
}

func (ds *DISTIL) StreamFromPath(path string) *Stream {
	//Resolve path to uuid and call stream from uuid
	var result bson.M = findAssertOne(ds.col, "Path", path)

	if result == nil {
		return nil
	}

	uuidstrint, ok := result["uuid"]
	if !ok {
		panic(fmt.Sprintf("Document for Path %s is missing required field 'uuid'", path))
	}

	uuidstr, ok := uuidstrint.(string)
	if !ok {
		panic(fmt.Sprintf("Value of UUID for stream with Path %s is not a string", path))
	}

	var id = uuid.Parse(uuidstr)
	if id == nil {
		panic(fmt.Sprintf("Document for Path %s has invalid UUID %s", path, uuidstr))
	}

	return &Stream{ds: ds, id: id, path: path}
}

func (ds *DISTIL) StreamsFromPaths(paths []string) []*Stream {
	//loop over StreamFromPath
	var streams = make([]*Stream, len(paths))
	for i, path := range paths {
		streams[i] = ds.StreamFromPath(path)
	}
	return streams
}

// This is the same as StreamFromPath if
// the path exists, otherwise it creates
// a new stream with that path (and a new uuid)
// and returns it.
/* NOTE: This function should NOT be called concurrently with the same PATH. */
func (ds *DISTIL) MakeOrGetByPath(path string) *Stream {
	//if stream does not exist then create metadata for
	//the stream
	//see https://github.com/immesys/distil-spark/blob/master/src/scala/io/btrdb/distil/dsl.scala#L173
	//and create the metadata a bit like that (assu)

	var stream *Stream = ds.StreamFromPath(path)
	if stream != nil {
		return stream
	}
	var id uuid.UUID = uuid.NewRandom()
	var metadata = bson.M{
		"uuid": id.String(),
		"Path": path,
		"Properties": bson.M{
			"Timezone":      "America/Los_Angeles",
			"UnitofMeasure": "Unspecified",
			"UnitofTime":    "ns",
			"ReadingType":   "double",
		},
		"Metadata": bson.M{
			"SourceName": "DISTIL",
		},
	}

	var err error = ds.col.Insert(metadata)
	if err != nil {
		panic(err)
	}

	return &Stream{ds: ds, id: id, path: path}
}
func (ds *DISTIL) MakeOrGetByPaths(paths []string) []*Stream {
	var streams = make([]*Stream, len(paths))
	for i, path := range paths {
		streams[i] = ds.MakeOrGetByPath(path)
	}
	return streams
}

func (s *Stream) TagVersion(uniqueName string) uint64 {
	//Get the metadata key from this stream
	// distil.<uniquename>
	//and parse it as int
	//panic on any error
	var result bson.M = findAssertOne(s.ds.col, "Path", s.path)
	if result == nil {
		panic(fmt.Sprintf("Could not find document for Path %s", s.path))
	}

	distilint, ok := result["distil"]
	if !ok {
		return 1
	}
	distil, ok := distilint.(bson.M)
	if !ok {
		panic(fmt.Sprintf("Document for Path %s has 'distil' key not mapped to object", s.path))
	}
	valint, ok := distil[uniqueName]
	if !ok {
		return 1
	}
	val, ok := valint.(int64)
	if !ok {
		panic(fmt.Sprintf("Value for TagVersion of distillate %s for stream with Path %s is not an int64", uniqueName, s.path))
	}
	return uint64(val)
}

func (s *Stream) SetTagVersion(uniqueName string, version uint64) {
	var selector = bson.M{
		"uuid": s.id.String(),
	}
	var metadata = bson.M{
		"$set": bson.M{
			fmt.Sprintf("distil.%s", uniqueName): version,
		},
	}

	var err error = s.ds.col.Update(selector, metadata)
	if err != nil {
		panic(err)
	}

}

func (s *Stream) ChangesBetween(oldversion uint64, newversion uint64) []TimeRange {
	//Do the btrdb query, read the results from the chan into a slice
	//panic on any error
	var trslice = make([]TimeRange, 0, 20)
	var trc chan btrdb.TimeRange
	var tr btrdb.TimeRange
	var errc chan string
	var erri error
	var errstr string

	trc, _, errc, erri = s.ds.bdb.QueryChangedRanges(s.id, oldversion, newversion, CHANGED_RANGE_RES)
	if erri != nil {
		panic(erri)
	}
	for tr = range trc {
		trslice = append(trslice, TimeRange{Start: tr.StartTime, End: tr.EndTime})
	}

	errstr = <-errc
	if errstr != "" {
		panic(errstr)
	}

	return trslice
}

func (s *Stream) GetPoints(r TimeRange, rebase Rebaser, version uint64) []Point {
	//feed the resulting channel through rebase.Process and turn it into
	//a []Point slice
	var ptslice = make([]Point, 0, (r.End-r.Start)*130/1000000000)

	var pt btrdb.StandardValue
	var ptc chan btrdb.StandardValue
	var errc chan string
	var erri error
	var errstr string

	ptc, _, errc, erri = s.ds.bdb.QueryStandardValues(s.id, r.Start, r.End, version)
	if erri != nil {
		panic(erri)
	}

	var rbc chan btrdb.StandardValue = rebase.Process(r.Start, r.End, ptc)

	for pt = range rbc {
		ptslice = append(ptslice, Point{T: pt.Time, V: pt.Value})
	}

	errstr = <-errc // Maybe I should change this into a nonblocking read() using select?
	if errstr != "" {
		panic(erri)
	}

	return ptslice
}

func (s *Stream) EraseRange(r TimeRange) {
	var statc chan string
	var stat string
	var erri error

	statc, erri = s.ds.bdb.DeleteValues(s.id, r.Start, r.End)
	if erri != nil {
		panic(erri)
	}

	stat = <-statc
	if stat != "ok" {
		panic(fmt.Sprintf("Status code from BTrDB on Delete is %s", stat))
	}
}

func (s *Stream) WritePoints(p []Point) {
	var statc chan string
	var stat string
	var erri error

	var sv = make([]btrdb.StandardValue, len(p))
	for i, point := range p {
		sv[i] = btrdb.StandardValue{Time: point.T, Value: point.V}
	}

	statc, erri = s.ds.bdb.InsertValues(s.id, sv, false)
	if erri != nil {
		panic(erri)
	}

	stat = <-statc
	if stat != "ok" {
		panic(fmt.Sprintf("Status code from BTrDB on Insert is %s", stat))
	}
}

func (s *Stream) CurrentVersion() uint64 {
	var vr uint64
	var vrc chan uint64
	var errstr string
	var errc chan string
	var erri error

	vrc, errc, erri = s.ds.bdb.QueryVersion([]uuid.UUID{s.id})
	if erri != nil {
		panic(erri)
	}

	vr = <-vrc
	errstr = <-errc

	if errstr != "" {
		panic(fmt.Sprintf("Status from BTrDB on Version Query is %s", errstr))
	}

	return vr
}

func (s *Stream) Exists() bool {
	var vr uint64
	var vrc chan uint64
	var errstr string
	var errc chan string
	var erri error

	vrc, errc, erri = s.ds.bdb.QueryVersion([]uuid.UUID{s.id})
	if erri != nil {
		panic(erri)
	}

	vr = <-vrc
	errstr = <-errc

	if errstr != "" {
		return false
	}

	return true
}
