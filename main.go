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
const CNAME = "metadata2"
const MaxVersionSet = 100

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
	h.inputs = ds.StreamsFromPaths(h.reg.InputPaths)
	h.outputs = ds.MakeOrGetByPaths(h.reg.OutputPaths)
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
		then := time.Now()

		versions := make([]uint64, len(h.inputs))
		headversions := make([]uint64, len(h.inputs))
		some := false
		for idx, in := range h.inputs {
			versions[idx] = in.TagVersion(h.reg.UniqueName)
			headversions[idx] = in.CurrentVersion()
			if headversions[idx]-versions[idx] > MaxVersionSet {
				headversions[idx] = versions[idx] + MaxVersionSet
			}
			if headversions[idx] != versions[idx] {
				some = true
			}
		}

		if !some {
			fmt.Printf("NOP %s \n", h.reg.UniqueName)
			time.Sleep(5 * time.Second)
			continue
		}

		//Find the changed ranges
		chranges := make([]TimeRange, 0, 20)
		for idx, in := range h.inputs {
			fmt.Println("INF Adding range for versions", versions[idx], "to", headversions[idx])
			chranges = append(chranges, in.ChangesBetween(versions[idx], headversions[idx])...)
		}
		lastt := int64(0)

		//Add merge
		merged_ranges := expandPrereqsParallel(chranges)
		for _, r := range merged_ranges {
			if r.End > lastt {
				lastt = r.End
			}
			//Query the changed data and make blocks
			is := InputSet{
				startIndexes: make([]int, len(h.inputs)),
				samples:      make([][]Point, len(h.inputs)),
				tr:           r,
			}
			originalStartTime := r.Start
			r.Start -= h.d.LeadNanos()
			subthen := time.Now()
			fmt.Printf("INF Querying inputs for range at %s\n", time.Unix(0, r.Start))
			total := 0
			for idx, in := range h.inputs {
				is.samples[idx] = in.GetPoints(r, h.d.Rebase(), headversions[idx])
				total += len(is.samples[idx])
				//Find the index of the original start of range
				is.startIndexes[idx] = len(is.samples[idx])
				for search := 0; search < len(is.samples[idx]); search++ {
					if is.samples[idx][search].T >= originalStartTime {
						is.startIndexes[idx] = search
						break
					}
				}
			}
			fmt.Printf("INF Query finished (%d points, %d seconds)\n", total, time.Now().Sub(subthen)/time.Second)
			//Create the output data blocks
			allocHint := 5000
			for _, in := range is.samples {
				if len(in) > allocHint {
					allocHint = len(in) + 1000
				}
			}
			os := OutputSet{
				outbufs: make([][]Point, len(h.outputs)),
			}
			for idx := range h.outputs {
				os.outbufs[idx] = make([]Point, 0, allocHint)
			}
			os.ownership = is.tr //By default

			//Process
			h.d.Process(&is, &os)

			//Write back the data
			for idx, ostream := range h.outputs {
				ostream.EraseRange(os.ownership)
				ostream.WritePoints(os.outbufs[idx])
			}
		}

		//Update the tag version
		for idx, in := range h.inputs {
			in.SetTagVersion(h.reg.UniqueName, headversions[idx])
		}

		fmt.Printf("FIN %s \n  >> latest at %s\n  >> took %.2f seconds to compute\n",
			h.reg.UniqueName, time.Unix(0, lastt), float64(time.Now().Sub(then)/time.Millisecond)/1000.0)
	}
}

func FromEnvVars() (string, string) {
	btrdbAddr := os.Getenv("DISTIL_BTRDB_ADDR")
	mongoAddr := os.Getenv("DISTIL_MONGO_ADDR")
	if btrdbAddr == "" {
		fmt.Println("WARN: ENV $DISTIL_BTRDB_ADDR not set, using 'localhost:4410'")
		btrdbAddr = "localhost:4410"
	}
	if mongoAddr == "" {
		fmt.Println("WARNL: ENV $DISTIL_MONGO_ADDR not set, using 'localhost:27017'")
		mongoAddr = "localhost:27017"
	}
	return btrdbAddr, mongoAddr
}

type InputSet struct {
	startIndexes []int
	samples      [][]Point
	tr           TimeRange
}
type Point struct {
	T int64
	V float64
}

func (is *InputSet) Get(stream int, sample int) Point {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
		//os.Exit(1)
	}
	realSample := sample + is.startIndexes[stream]
	if realSample < 0 || realSample >= len(is.samples[stream]) {
		panic(fmt.Sprintf("Distillate attempted to access sample outside InputSet.\nstream=%d sample=%d realsample=%d", stream, sample, realSample))
		//os.Exit(1)
	}
	return is.samples[stream][realSample]
}
func (is *InputSet) NumSamples(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
		//os.Exit(1)
	}
	return len(is.samples[stream]) - is.startIndexes[stream]
}
func (is *InputSet) NumLeadSamples(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
		//os.Exit(1)
	}
	return is.startIndexes[stream]
}
func (is *InputSet) GetRange() TimeRange {
	return is.tr
}

type OutputSet struct {
	outbufs   [][]Point
	ownership TimeRange
}

func (oss *OutputSet) AddPoint(stream int, p Point) {
	if stream < 0 || stream >= len(oss.outbufs) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside OutputSet: %d", stream))
		//os.Exit(1)
	}
	if p.T < oss.ownership.Start || p.T >= oss.ownership.End {
		panic("Distillate attempted to write outside its Range")
		//os.Exit(1)
	}
	oss.outbufs[stream] = append(oss.outbufs[stream], p)
}
func (oss *OutputSet) Add(stream int, time int64, val float64) {
	oss.AddPoint(stream, Point{time, val})
}
func (oss *OutputSet) SetRange(r TimeRange) {
	oss.ownership = r
}
