package distil

import (
	"github.com/SoftwareDefinedBuildings/btrdb-go"
	"github.com/pborman/uuid"
)

type Stream struct {
	ds   *DISTIL
	id   uuid.UUID
	path string
}

func StreamFromUUID(id uuid.UUID) *Stream {
	//TODO sam
	//return nil if it doesn't exist
}

func StreamsFromUUIDs(ids []uuid.UUID) []*Stream {
	//loop over above
}

func StreamFromPath(path string) *Stream {
	//Resolve path to uuid and call stream from uuid
}

func StreamsFromPaths(paths []string) []*Stream {
	//loop over StreamFromPath
}

// This is the same as StreamFromPath if
// the path exists, otherwise it creates
// a new stream with that path (and a new uuid)
// and returns it.
func MakeOrGetByPath(path string) *Stream {
	//TODO sam
	//if stream does not exist then create metadata for
	//the stream
	//see https://github.com/immesys/distil-spark/blob/master/src/scala/io/btrdb/distil/dsl.scala#L173
	//and create the metadata a bit like that (assu)
}
func MakeOrGetByPaths(paths []string) []*Stream {
	//Loop over above
}

func (s *Stream) TagVersion(uniqueName string) int64 {
	//TODO sam
	//Get the metadata key from this stream
	// distil.<uniquename>
	//and parse it as int
	//panic on any error
}
func (s *Stream) SetTagVersion(uniqueName string, version int64) {
	//TODO sam set as above
}
func (s *Stream) ChangesSince(version int64) []btrdb.TimeRange {
	//TODO sam
	//Do the btrdb query, read the results from the chan into a slice
	//panic on any error
}
