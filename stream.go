package distil

import (
	"fmt"

	"github.com/pborman/uuid"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type Stream struct {
	ds   *DISTIL
	id   uuid.UUID
	path string
}

func findAssertOne(col *mgo.Collection, key string, val string) bson.M {
	var q *mgo.Query = ds.col.Find(bson.M{key: value})
	c, err := q.Count()
	if err != nil {
		panic(err)
	} else if c == 0 {
		return nil
	} else if c != 1 {
		panic(fmt.Sprintf("Multiple streams with %s = %s", key, value))
	}

	var result bson.M
	err = q.One(result)
	if err != nil {
		panic(err)
	}

	return value
}

func (ds *DISTIL) StreamFromUUID(id uuid.UUID) *Stream {
	//TODO sam
	//return nil if it doesn't exist
	var result bson.M = findAssertOne(ds.col, "uuid", id.String())

	path, ok := result["Path"]
	if !ok {
		panic(fmt.Sprintf("Document for UUID %s is missing required field 'Path'", id.String()))
	}

	return &Stream{ds: ds, id: id, path: path}
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

	uuidstr, ok := result["uuid"]
	if !ok {
		panic(fmt.Sprintf("Document for Path %s is missing required field 'uuid'", path))
	}

	var id = uuid.Parse(uuidstr)
	if id != nil {
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
	//TODO sam
	//if stream does not exist then create metadata for
	//the stream
	//see https://github.com/immesys/distil-spark/blob/master/src/scala/io/btrdb/distil/dsl.scala#L173
	//and create the metadata a bit like that (assu)

	var stream *stream = StreamFromPath(path)
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
			"SourceName":    "BTS",
		},
		"Metadata": bson.M{},
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

func (s *Stream) TagVersion(uniqueName string) int64 {
	//TODO sam
	//Get the metadata key from this stream
	// distil.<uniquename>
	//and parse it as int
	//panic on any error
	var result bson.M = findAssertOne(s.ds.col, "Path", s.path)

	distilint, ok := result["distil"]
	if !ok {
		panic(fmt.Sprintf("Document for Path %s is missing required field 'distil'", s.Path))
	}
	distil, ok := distilint.(bson.M)
	if !ok {
		panic(fmt.Sprintf("Document for Path %s has 'distil' key not mapped object", s.Path))
	}
	valint, ok := distil[uniqueName]
	if !ok {
		panic(fmt.Sprintf("Document for distillate %s not found for stream with Path %s", uniqueName, s.Path))
	}
	val, ok := valint.(int64)
	if !ok {
		panic(fmt.Sprintf("Value for TagVersion of distillate %s for stream with Path %s is not an int64", uniquename, s.Path))
	}
	return val
}

func (s *Stream) SetTagVersion(uniqueName string, version int64) {
	//TODO sam set as above
	var metadata = bson.M{
		"$set": bson.M{
			fmt.Sprintf("distil.%s", uniqueName): version,
		},
	}

}

func (s *Stream) ChangesSince(version int64) []TimeRange {
	//TODO sam
	//Do the btrdb query, read the results from the chan into a slice
	//panic on any error
}

func (s *Stream) GetPoints(r TimeRange, rebase Rebaser, version int64) []Point {
	//TODO sam
	//feed the resulting channel through rebase.Process and turn it into
	//a []Point slice
}

func (s *Stream) EraseRange(r TimeRange) {
	//TODO sam
}

func (s *Stream) WritePoints(p []Point) {
	//TODO sam
}

func (s *Stream) CurrentVersion() int64 {
	//TODO sam
}

func (s *Stream) ChangesBetween(fromV, toV int64) []TimeRange {
	//TODO sam
}
