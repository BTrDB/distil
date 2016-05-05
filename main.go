package distil

import (
	"fmt"
	"os"
	"time"

	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
	"github.com/pborman/uuid"
	"gopkg.in/mgo.v2"
)

const DBNAME = "qdf"
const CNAME = "metadata"

func chk(e error) {
	if e != nil {
		fmt.Println("Error:", e)
		os.Exit(1)
	}
}

type DISTIL struct {
	col         *mgo.Collection
	bdb         *btrdb.BTrDBConnection
	distillates []*handle
}

func NewDISTIL(btrdbaddr string, mongoaddr string) *DISTIL {
	rv := DISTIL{}
	// Init mongo
	ses, err := mgo.Dial(mongoaddr)
	chk(err)
	db := ses.DB(DBNAME)
	rv.col = db.C(CNAME)

	// Init btrdb
	rv.bdb, err = btrdb.NewBTrDBConnection(btrdbaddr)
	chk(err)
	return &rv
}

func (ds *DISTIL) Resolve(path string) uuid.UUID {
	//For sam to do
	return uuid.NewUUID()
}

func (ds *DISTIL) ResolveAll(paths []string) []uuid.UUID {
	rv := make([]uuid.UUID, len(paths))
	for i := 0; i < len(rv); i++ {
		rv[i] = ds.Resolve(paths[i])
	}
	return rv
}

type Registration struct {
	Instance    Distillate
	UniqueName  string
	InputPaths  []string
	OutputPaths []string
}

type handle struct {
	d       Distillate
	reg     Registration
	inputs  []*Stream
	outputs []*Stream
}

func (ds *DISTIL) RegisterDistillate(r *Registration) {
	if r.UniqueName == "" {
		fmt.Println("Aborting. Cannot register a distillate with no UniqueName")
		os.Exit(1)
	}
	if r.Instance == nil {
		fmt.Println("Aborting. Cannot register a distillate with no Instance")
		os.Exit(1)
	}
	h := handle{
		d:   r.Instance,
		reg: *r,
	}
	ds.distillates = append(ds.distillates, &h)
}

func (ds *DISTIL) StartEngine() {
	for _, h := range ds.distillates {
		go h.ProcessLoop()
	}
	for {
		time.Sleep(10 * time.Second)
	}
}

func (h *handle) ProcessLoop() {
	for {
		//Find the last version
	/*	lastver := 0
		for _, in := range h.inputs {
			tagver := in.GetTagVersion(h.UniqueName)
			if lowestver == 0 || (tagver != 0 && tagver < lastver) {
				lastver = tagver
			}
		}*/
		//Find the changed ranges
		chranges := []btrdb.TimeRange
    for _, in := range h.inputs {
      chranges = append(chranges, in.ChangesSinceTag(h.UniqueName)...)
    }

    //Add merge
    merged_ranges := chranges


		for _, r := range merged_ranges {
      //Query the changed data and make blocks
      
    }

		//Process the data

		//Write it back

		//Update the tag version
		for _, in := range h.inputs {
			in.SetTagVersion(h.UniqueName, lastver)
		}
	}
}
