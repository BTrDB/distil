package distil

import (
	"context"
	"fmt"
	"os"
	"time"

	btrdb "gopkg.in/BTrDB/btrdb.v4"
)

// This is the maximum number of versions that will be processed at once
const MaxVersionSet = 100

func chk(e error) {
	if e != nil {
		fmt.Println("Error:", e)
		os.Exit(1)
	}
}

// DISTIL is a handle to the distil engine, including it's connections
// to BTrDB
type DISTIL struct {
	bdb         *btrdb.BTrDB
	distillates []*handle
}

// NewDISTIL creates a DISTIL handle by connecting to the given
// BTrDB and MongoDB addresses
func NewDISTIL() *DISTIL {
	rv := DISTIL{}
	// Init mongo
	var err error
	rv.bdb, err = btrdb.Connect(context.Background(), btrdb.EndpointsFromEnv()...)
	chk(err)
	return &rv
}

func (d *DISTIL) BTrDBConn() *btrdb.BTrDB {
	return d.bdb
}

// func (ds *DISTIL) Resolve(path string) uuid.UUID {
// 	//For sam to do
// 	return uuid.NewUUID()
// }
//
// func (ds *DISTIL) ResolveAll(paths []string) []uuid.UUID {
// 	rv := make([]uuid.UUID, len(paths))
// 	for i := 0; i < len(rv); i++ {
// 		rv[i] = ds.Resolve(paths[i])
// 	}
// 	return rv
// }

// Registration is a handle to a specific instance of a distillate, along
// with the information required to prepare it it
type Registration struct {
	Instance    Distillate
	UniqueName  string
	InputPaths  []string
	OutputPaths []string
	OutputUnits []string
}

type handle struct {
	d       Distillate
	reg     Registration
	inputs  []*Stream
	outputs []*Stream
}

// RegisterDistillate needs to be called once per instance of distillate
// with a populated Registration struct. Do not reuse the Registration
// struct, it is owned by the engine after this call.
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
	r.Instance.SetEngine(ds)
	h.inputs = ds.StreamsFromPaths(h.reg.InputPaths)
	h.outputs = ds.MakeOrGetByPaths(h.reg.OutputPaths, h.reg.OutputUnits)
	ds.distillates = append(ds.distillates, &h)
}

// StartEngine begins processing distillates. It does not return
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
			//10 is an uncreated stream, and BTrDB will return all of time as the
			//resulting changed range. That causes problems
			if headversions[idx] == 10 {
				continue
			}
			fmt.Printf("INF[%s] Adding range for versions %v to %v\n", h.reg.UniqueName, versions[idx], headversions[idx])
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
			mins := (r.End - r.Start) / int64(60*1e9)
			if mins > 60*24*30 {
				fmt.Printf("CRITICAL[%s] aborting this distillate run due to abnornal changed range\n", h.reg.UniqueName)
				return
			}
			fmt.Printf("INF[%s] Querying inputs for range at %s (%d minutes)\n", h.reg.UniqueName, time.Unix(0, r.Start), mins)
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
			fmt.Printf("INF[%s] Query finished (%d points, %d seconds)\n", h.reg.UniqueName, total, time.Now().Sub(subthen)/time.Second)
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

			fmt.Printf("INF[%s] Process finished\n", h.reg.UniqueName)

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

// An InputSet is passed to the Process method of a distillate, it contains
// preloaded data for the changed time range (plus any lead time).
type InputSet struct {
	startIndexes []int
	samples      [][]Point
	tr           TimeRange
}

// A Point is the primitive telemetry data type
type Point struct {
	// Time since the Unix epoch in nanoseconds
	T int64
	// Value
	V float64
}

// Get a data point from the InputSet, stream is an index into the InputPaths
// declared in the registration, sample is the point you wish to get. Sample
// 0 is the first sample in the changed range. Negative indices are lead samples
// (see LeadNanos) that can be used for context.
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

// Get the number of positive samples in the given stream
func (is *InputSet) NumSamples(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
		//os.Exit(1)
	}
	return len(is.samples[stream]) - is.startIndexes[stream]
}

// Get the number of negative samples (lead samples) in the given stream
func (is *InputSet) NumLeadSamples(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
		//os.Exit(1)
	}
	return is.startIndexes[stream]
}

// Get the time range that has changed that is being processed. This does not include
// lead time
func (is *InputSet) GetRange() TimeRange {
	return is.tr
}

// OutputSet is a handle onto the output streams and is used for writing back data
// from processing
type OutputSet struct {
	outbufs   [][]Point
	ownership TimeRange
}

// Add a point to the given stream
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

// A utility function, this constructs a point and calls AddPoint
func (oss *OutputSet) Add(stream int, time int64, val float64) {
	oss.AddPoint(stream, Point{time, val})
}

// Set the time range that this OutputSet is responsible for. This must be
// done before any points are added, and any points outside this range will
// be discarded. Any points that existed in the stream before the current
// Process that lie within this range will be deleted and replaced by the
// data in the current output set
func (oss *OutputSet) SetRange(r TimeRange) {
	oss.ownership = r
}

// This represents a range of time from Start (inclusive) to End (exclusive)
type TimeRange struct {
	Start int64
	End   int64
}

func maxInt64(x int64, y int64) int64 {
	if x > y {
		return x
	} else {
		return y
	}
}

/* Translation of Michael's code at
 * https://github.com/immesys/distil-spark/blob/master/src/scala/io/btrdb/distil/distiller.scala#L139
 * NOTE: This function may modify the input slice's contents (but not its length).
 */
func expandPrereqsParallel(changedRanges []TimeRange) []TimeRange {
	var ranges = changedRanges
	var thirdfield = make([]bool, len(changedRanges), len(changedRanges))
	var combinedRanges = make([]TimeRange, 0, len(changedRanges))

	var notDone = true
	for notDone {
		var progress = false
		var combined = false
		var minidx = 0

		var i int
		for i = 0; i < len(ranges); i++ {
			if !thirdfield[i] {
				progress = true
				// If another range starts before minidx
				if thirdfield[minidx] || ranges[i].Start < ranges[minidx].Start {
					minidx = i
				}
			}
		}

		// Now see if any other ranges' starts lie before the end of min
		for i = 0; i < len(ranges); i++ {
			if !thirdfield[i] && i != minidx && ranges[i].Start <= ranges[minidx].End {
				// This range's start lies before the end of min
				// Set minidx's end ot the max of the new range and min's end
				ranges[minidx] = TimeRange{Start: ranges[minidx].Start, End: maxInt64(ranges[minidx].End, ranges[i].End)}
				thirdfield[minidx] = false
				// Remove the new range (it is subsumed)
				ranges[i] = TimeRange{}
				thirdfield[i] = true
				combined = true
			}
		}
		if !progress {
			notDone = false
		} else if !combined {
			combinedRanges = append(combinedRanges, ranges[minidx])
			ranges[minidx] = TimeRange{}
			thirdfield[minidx] = true
		}
	}

	return combinedRanges
}
