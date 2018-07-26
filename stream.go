package distil

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/SoftwareDefinedBuildings/btrdb/bte"
	"github.com/pborman/uuid"
	btrdb "gopkg.in/btrdb.v4"
)

const changedRangeRes uint8 = 38

// Stream represents a handle on a specific stream and can be used for
// querying and inserting data on it. You should not need to use this
// directly
type Stream struct {
	ds *DISTIL
	s  *btrdb.Stream
}

// func findAssertOne(col *mgo.Collection, key string, value string) bson.M {
// 	var q *mgo.Query = col.Find(bson.M{key: value})
// 	c, err := q.Count()
// 	if err != nil {
// 		panic(err)
// 	} else if c == 0 {
// 		return nil
// 	} else if c != 1 {
// 		panic(fmt.Sprintf("Multiple streams with %s = %s", key, value))
// 	}
//
// 	var result bson.M
// 	err = q.One(&result)
// 	if err != nil {
// 		panic(err)
// 	}
//
// 	return result
// }

// Obtain a Stream given a UUID
func (ds *DISTIL) StreamFromUUID(id uuid.UUID) *Stream {
	s := ds.bdb.StreamFromUUID(id)

	return &Stream{ds: ds, s: s}
}

// ListUpmuPaths will get a list of all prefixes that look like they have a uPMU under
// them in the metadata. Note that these streams may not exist or may be empty.
// The heuristic is to look for L1MAG streams with a "uPMU" SourceName, and then strip
// off the suffix
// func (ds *DISTIL) ListUpmuPaths() []string {
// 	q := ds.col.Find(bson.M{"Metadata.SourceName": "uPMU", "Path": bson.M{"$regex": ".*L1MAG"}})
// 	rv := []string{}
// 	iter := q.Iter()
// 	var ob struct {
// 		Path string `bson:"Path"`
// 	}
// 	for iter.Next(&ob) {
// 		rv = append(rv, strings.TrimSuffix(ob.Path, "/L1MAG"))
// 	}
// 	return rv
// }

// This is similar to ListUpmuPaths but it will query BTrDB to see how many
// points are in each L1MAG stream and it will not return the path if the
// stream is empty
// func (ds *DISTIL) ListExistingUpmuPaths() []string {
// 	q := ds.col.Find(bson.M{"Metadata.SourceName": "uPMU", "Path": bson.M{"$regex": ".*L1MAG"}})
// 	rv := []string{}
// 	iter := q.Iter()
// 	var ob struct {
// 		Id   string `bson:"uuid"`
// 		Path string `bson:"Path"`
// 	}
// 	for iter.Next(&ob) {
// 		uu := uuid.Parse(ob.Id)
// 		ss := ds.StreamFromUUID(uu)
// 		if ss == nil {
// 			fmt.Println("Warning, bad stream: ", ob.Path)
// 			fmt.Println("UUID ", uu)
// 			continue
// 		}
// 		if ss == nil || !ss.Exists() {
// 			fmt.Println("INF Skipping empty stream:", ob.Path)
// 			continue
// 		}
// 		rv = append(rv, strings.TrimSuffix(ob.Path, "/L1MAG"))
// 	}
// 	return rv
// }

// Obtain a slice of streams corresponding to the given UUIDs
func (ds *DISTIL) StreamsFromUUIDs(ids []uuid.UUID) []*Stream {
	//loop over above
	var streams = make([]*Stream, len(ids))
	for i, id := range ids {
		streams[i] = ds.StreamFromUUID(id)
	}
	return streams
}

// Obtain a stream based on a path
func (ds *DISTIL) StreamFromPath(path string) *Stream {

	lastsepidx := strings.LastIndex(path, "/")
	name := path[lastsepidx+1:]
	collection := path[:lastsepidx]
	fmt.Printf("DEBUG: name=%q coll=%q\n", name, collection)

	streamz, err := ds.bdb.LookupStreams(context.Background(), collection, false, btrdb.OptKV("name", name), nil)
	if err != nil {
		if btrdb.ToCodedError(err).Code == 404 {
			return nil
		}
		panic(fmt.Sprintf("Unexpected error getting stream: %v", err))
	}
	if len(streamz) == 0 {
		return nil
	}
	if len(streamz) != 1 {
		panic(fmt.Sprintf("Lookup of stream %q/%q yielded %d results", collection, name, len(streamz)))
	}
	return &Stream{ds: ds, s: streamz[0]}

}

// Obtain multiple streams based on paths
func (ds *DISTIL) StreamsFromPaths(paths []string) []*Stream {
	//loop over StreamFromPath
	var streams = make([]*Stream, len(paths))
	for i, path := range paths {
		streams[i] = ds.StreamFromPath(path)
		if streams[i] == nil {
			fmt.Printf("could not locate stream %q\n", path)
			os.Exit(1)
		}
	}
	return streams
}

// This is the same as StreamFromPath, but if the path does not exist, it will
// create the stream
// NOTE: This function should NOT be called concurrently with the same path.
func (ds *DISTIL) MakeOrGetByPath(path string, unit string) *Stream {
	var stream *Stream = ds.StreamFromPath(path)
	if stream != nil {
		return stream
	}
	var id uuid.UUID = uuid.NewRandom()
	lastsepidx := strings.LastIndex(path, "/")
	name := path[lastsepidx+1:]
	collection := path[:lastsepidx]
	s, err := ds.bdb.Create(context.Background(), id, collection, btrdb.M{"name": name, "unit": unit}, nil)
	if err != nil {
		panic(err)
	}
	return &Stream{ds: ds, s: s}
}

// Same as MakeOrGetByPath but does multiple
func (ds *DISTIL) MakeOrGetByPaths(paths []string, units []string) []*Stream {
	var streams = make([]*Stream, len(paths))
	if len(paths) != len(units) {
		panic(fmt.Sprintf("Arguments to MakeOrGetByPaths must be the same length, got %d / %d", len(paths), len(units)))
	}
	for i, path := range paths {
		streams[i] = ds.MakeOrGetByPath(path, units[i])
	}
	return streams
}

// Get the last version of the stream that uniqueName processed
func (s *Stream) TagVersion(uniqueName string) uint64 {
	kn := "distil." + uniqueName

	annz, _, err := s.s.Annotations(context.Background())
	if err != nil {
		panic(err)
	}
	ver, ok := annz[kn]
	if !ok {
		return 0
	}
	iver, err := strconv.ParseInt(ver, 10, 64)
	if err != nil {
		panic(err)
	}
	return uint64(iver)
}

// Set the last version of the stream that uniqueName processed
func (s *Stream) SetTagVersion(uniqueName string, version uint64) {
	for i := 0; i < 10; i++ {
		kn := "distil." + uniqueName
		_, aver, err := s.s.Annotations(context.Background())
		if err != nil {
			panic(err)
		}

		err = s.s.CompareAndSetAnnotation(context.Background(), aver, btrdb.OptKV(kn, fmt.Sprintf("%d", version)))
		if err != nil {
			if btrdb.ToCodedError(err).Code == bte.AnnotationVersionMismatch {
				continue
			}
			panic(err)
		} else {
			return
		}
	}
}

// Obtain the changed ranges between the two versions
func (s *Stream) ChangesBetween(oldversion uint64, newversion uint64) []TimeRange {
	//Do the btrdb query, read the results from the chan into a slice
	//panic on any error
	var trslice = make([]TimeRange, 0, 20)

	ccr, _, cerr := s.s.Changes(context.Background(), oldversion, newversion, changedRangeRes)
	for cr := range ccr {
		trslice = append(trslice, TimeRange{Start: cr.Start, End: cr.End})
	}
	if err := <-cerr; err != nil {
		panic(err)
	}
	return trslice
}

// Get points from the stream, applying the given rebase
func (s *Stream) GetPoints(r TimeRange, rebase Rebaser, version uint64) []Point {
	//feed the resulting channel through rebase.Process and turn it into
	//a []Point slice
	var ptslice = make([]Point, 0, (r.End-r.Start)*130/1000000000)

	crv, _, cerr := s.s.RawValues(context.Background(), r.Start, r.End, version)

	rbc := rebase.Process(r.Start, r.End, crv)
	for pt := range rbc {
		ptslice = append(ptslice, Point{T: pt.Time, V: pt.Value})
	}
	if err := <-cerr; err != nil {
		panic(err)
	}
	return ptslice
}

// Erase everything in the stream that falls inside the given time range
func (s *Stream) EraseRange(r TimeRange) {
	_, err := s.s.DeleteRange(context.Background(), r.Start, r.End)
	if err != nil {
		panic(err)
	}
}

// Write the given points to the stream
func (s *Stream) WritePoints(pts []Point) {
	err := s.s.InsertF(context.Background(), len(pts),
		func(i int) int64 {
			return pts[i].T
		},
		func(i int) float64 {
			return pts[i].V
		})
	if err != nil {
		panic(err)
	}
}

// Get the current version of the stream
func (s *Stream) CurrentVersion() uint64 {
	ver, err := s.s.Version(context.Background())
	if err != nil {
		panic(err)
	}
	return ver
}

func (s *Stream) Exists() bool {
	rv, err := s.s.Exists(context.Background())
	if err != nil {
		panic(err)
	}
	return rv
}
