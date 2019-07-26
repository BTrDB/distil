package distil

import (
	"bufio"
	"bytes"
	"container/heap"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime/debug"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"net/http"
	_ "net/http/pprof"

	btrdb "github.com/BTrDB/btrdb"
	"github.com/BTrDB/btrdb/bte"
	"github.com/PingThingsIO/smartgridstore/modules/operators/distilclient"
	logging "github.com/op/go-logging"
	"github.com/pborman/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	etcd "go.etcd.io/etcd/clientv3"
)

var pmAssignmentInvocations prometheus.Counter
var pmInvocationErrors prometheus.Counter
var pmProcessedPoints prometheus.Counter
var pmInsertedPoints prometheus.Counter
var pmAssignmentTime prometheus.Histogram
var pmRawStageTime prometheus.Histogram
var pmRawStageTimeRatio prometheus.Histogram
var pmActiveAssignments prometheus.Gauge

const MaxBackOff = 30 * 60
const MinBackOff = 10

func init() {

	pmAssignmentInvocations = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distil",
		Name:      "assignmentinvocations",
		Help:      "The number of invocations done on assignments",
	})
	prometheus.MustRegister(pmAssignmentInvocations)

	pmInvocationErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distil",
		Name:      "invocation_errors",
		Help:      "The number of invocations that returned an error",
	})
	prometheus.MustRegister(pmInvocationErrors)

	pmProcessedPoints = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distil",
		Name:      "processed_points",
		Help:      "The number of points queried for distillates",
	})
	prometheus.MustRegister(pmProcessedPoints)

	pmInsertedPoints = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "distil",
		Name:      "inserted_points",
		Help:      "The number of points inserted from distillates",
	})
	prometheus.MustRegister(pmInsertedPoints)

	pmActiveAssignments = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "distil",
		Name:      "active_assignments",
		Help:      "The number of active assignments",
	})
	prometheus.MustRegister(pmActiveAssignments)

	pmAssignmentTime = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "distil",
		Name:       "assignment_time",
		Help:       "Microseconds spent per assignment invocation",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	prometheus.MustRegister(pmAssignmentTime)

	pmRawStageTime = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "distil",
		Name:       "raw_stage_time",
		Help:       "Microseconds spent per assignment invocation",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	prometheus.MustRegister(pmRawStageTime)

	pmRawStageTimeRatio = prometheus.NewSummary(prometheus.SummaryOpts{
		Namespace:  "distil",
		Name:       "raw_stage_time_ratio",
		Help:       "Ratio of assignment time to wall time",
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	prometheus.MustRegister(pmRawStageTimeRatio)
}

//The maximum number of sequential stages that can be present
const MaxStages = 10

var allOfTime TimeRange = TimeRange{Start: -(16 << 56), End: (48 << 56) - 1}

type assignmentState struct {
	LastAlgorithmVersion uint64
	LastVersions         []uint64
	TargetVersions       []uint64
	LastProcessAfter     int64
	LastProcessBefore    int64
	Cursortime           int64
	InProgress           bool
}

var lg *logging.Logger

func init() {
	logging.SetBackend(logging.NewLogBackend(os.Stdout, "", 0))
	logging.SetFormatter(logging.MustStringFormatter("[%{level:-8s}]%{time:2006-01-02T15:04:05.000000} > %{message}"))
	lg = logging.MustGetLogger("log")
}

// DISTIL is a handle to the distil engine, including it's connections
// to BTrDB
type DISTIL struct {
	etcd *etcd.Client

	activeStreams int
	assignments   []*Assignment

	alg Algorithm

	astatemu       sync.Mutex
	astates        map[[16]byte]*assignmentState
	pendingAstates map[[16]byte]*assignmentState

	dclient *distilclient.DistilClient

	//We maintain a separate BTrDB connection for each API key
	bdbzmu sync.Mutex
	bdbz   map[string]*btrdb.BTrDB

	//The heap that schedules work on assignments
	aheap   AssignmentHeap
	aheapmu sync.Mutex
	//A serial number to ensure round-robin on assignment execution
	aserial uint64
	//We recycle the QueuedAssignment objects to reduce allocation
	qapool sync.Pool
	//A shallow queue from the front of the heap to the workers
	workq chan *Assignment
}

//An assignment is a specific instantiation of a distillate algorithm
//with its set of inputs and outputs
type Assignment struct {
	//The internal assignment object given to us from distilclient
	ua *distilclient.Assignment

	Instance string
	Subname  string
	//A hash of Instance/Subname
	ID uuid.UUID
	//These are the instance parameters
	Parameters map[string]string
	Inputs     []distilclient.AssignmentStream
	//The extra map returned from the selector
	Extra map[string]string

	//Data will only be processed between these timestamps
	ProcessAfter  int64
	ProcessBefore int64

	inputs    []*btrdb.Stream
	outputs   []*btrdb.Stream
	algorithm Algorithm
	//The config returned from the initial call to the algorithm
	cfg *Config
	ds  *DISTIL
	bdb *btrdb.BTrDB

	//The time we will sleep between invocations of this assignment
	//if it returns an error or NoData
	backoff int
}

//An Algorithm captures the processing that you want to do
type Algorithm interface {
	//Initial configuration of the DISTIL engine by the algorithm
	Config(a *Assignment) *Config
}

//For development, you may with to manually instantiate distil without
//pulling in the configuration from the operator
type ManualInstantiationParameters struct {
	BTrDBEndpoints []string
	EtcdEndpoint   string
	Assignments    []*Assignment
	Algorithm      Algorithm
	Workers        int
}

// func ManualInstantiate(p *ManualInstantiationParameters) (*DISTIL, error) {
// 	rv := DISTIL{}
// 	if len(p.BTrDBEndpoints) == 0 {
// 		p.BTrDBEndpoints = btrdb.EndpointsFromEnv()
// 	}
// 	if p.EtcdEndpoint == "" {
// 		p.EtcdEndpoint = os.Getenv("ETCD_ENDPOINT")
// 	}
// 	rv.connect(p.BTrDBEndpoints, p.EtcdEndpoint)
// 	rv.assignments = p.Assignments
// 	rv.alg = p.Algorithm
// 	rv.activeStreams = p.Workers
// 	err := rv.materializeAssignments()
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &rv, nil
// }

func Begin(i Algorithm) {

	astreams, err := strconv.ParseInt(os.Getenv("DISTIL_ACTIVE_STREAMS"), 10, 64)
	if err != nil || astreams <= 0 {
		lg.Fatalf("Invalid number of active streams (%v)", err)
	}

	//Start profiling
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe("0.0.0.0:6060", nil)
		lg.Errorf("HTTP listen error: %v", err)
	}()

	rv := DISTIL{
		activeStreams:  int(astreams),
		astates:        make(map[[16]byte]*assignmentState),
		pendingAstates: make(map[[16]byte]*assignmentState),
		bdbz:           make(map[string]*btrdb.BTrDB),
		workq:          make(chan *Assignment, astreams),
	}

	rv.qapool.New = func() interface{} {
		return &QueuedAssignment{}
	}

	rv.connect(os.Getenv("ETCD_ENDPOINT"))

	rv.alg = i

	go rv.feedAssignments()

	//rv.loadAssignments()
	//rv.materializeAssignments()
	go rv.writeAstateLoop()
	rv.StartEngine()
}

func (ds *DISTIL) writeAstateLoop() {
	for {
		time.Sleep(5 * time.Second)
		ds.astatemu.Lock()
		m := ds.pendingAstates
		ds.pendingAstates = make(map[[16]byte]*assignmentState)
		ds.astatemu.Unlock()
		for k, v := range m {
			err := ds.saveassignmentState(uuid.UUID(k[:]), v)
			if err != nil {
				lg.Panicf("error saving assignment state: %v", err)
			}
		}
	}
}

func (ds *DISTIL) loadassignmentState(asn uuid.UUID) (*assignmentState, error) {
	ds.astatemu.Lock()
	s, ok := ds.astates[asn.Array()]
	if ok {
		ds.astatemu.Unlock()
		return s, nil
	}
	defer ds.astatemu.Unlock()
	rvk, err := ds.etcd.Get(context.Background(), "distil/state/"+asn.String())
	if err != nil {
		return nil, err
	}
	if rvk.Count == 0 {
		return nil, nil
	}
	bin := rvk.Kvs[0].Value
	rv := &assignmentState{}
	err = gob.NewDecoder(bytes.NewBuffer(bin)).Decode(rv)
	if err != nil {
		panic(err)
	}
	ds.astates[asn.Array()] = rv
	return rv, nil
}

func (asn *Assignment) lazyWriteassignmentState(state *assignmentState) {
	asn.ds.astatemu.Lock()
	asn.ds.astates[asn.ID.Array()] = state
	asn.ds.pendingAstates[asn.ID.Array()] = state
	asn.ds.astatemu.Unlock()
}

func (ds *DISTIL) saveassignmentState(asn uuid.UUID, state *assignmentState) error {
	buf := bytes.Buffer{}
	err := gob.NewEncoder(&buf).Encode(state)
	if err != nil {
		panic(err)
	}
	_, err = ds.etcd.Put(context.Background(), "distil/state/"+asn.String(), string(buf.Bytes()))
	if err != nil {
		return err
	}
	return nil
}

func stableHash(name, subname, refname string) uuid.UUID {
	space := uuid.Parse("456c708a-2556-46ab-a45f-0a94d55d5c73")
	id := fmt.Sprintf("%s\x00%s\x00%s\x00", name, subname, refname)
	return uuid.NewSHA1(space, []byte(id))
}

func (ds *DISTIL) feedAssignments() error {
	ctx := context.Background()

	upstreamAssignments := ds.dclient.Announce()

nextassignment:
	for ua := range upstreamAssignments {
		lg.Infof("got assignment %v/%v", ua.Name, ua.Subname)
		pmActiveAssignments.Add(1)
		a := &Assignment{
			ua: ua,
		}
		//We copy all of these across rather than embedding the struct because
		//there are fields we don't want to expose to the user, such as
		//the LockContext
		a.Parameters = ua.Parameters
		apikey, ok := a.Parameters["apikey"]
		if !ok {
			a.Error("no apikey present, aborting")
			continue nextassignment
		}
		a.Extra = ua.Extra
		a.Instance = ua.Name
		a.Subname = ua.Subname
		a.algorithm = ds.alg
		a.ID = stableHash(a.Instance, a.Subname, "ID")
		a.bdb = ds.connectBDB(apikey)
		a.backoff = MinBackOff
		//Special parameters
		if pa, ok := a.Parameters["processAfter"]; ok {
			pai, err := strconv.ParseInt(pa, 10, 64)
			if err != nil {
				a.Error("could not parse processAfter parameter: %v", err)
				continue nextassignment
			}
			a.ProcessAfter = pai
		} else {
			a.ProcessAfter = btrdb.MinimumTime
		}
		if pb, ok := a.Parameters["processBefore"]; ok {
			pbi, err := strconv.ParseInt(pb, 10, 64)
			if err != nil {
				a.Error("could not parse processBefore parameter: %v", err)
				continue nextassignment
			}
			a.ProcessBefore = pbi
		} else {
			a.ProcessBefore = btrdb.MaximumTime
		}
		a.ds = ds
		a.inputs = make([]*btrdb.Stream, len(ua.InputStreams))
		for idx, is := range ua.InputStreams {
			s := a.bdb.StreamFromUUID(is.UUID)
			ex, err := s.Exists(ctx)
			if err != nil {
				return err
			}
			if !ex {
				a.Error("terminating this assignment: input stream %s does not exist", is)
				continue nextassignment
			}
			a.inputs[idx] = s
		}

		cfg, ok := configWrapper(a.algorithm.Config, a)
		if !ok {
			continue nextassignment
		}
		a.cfg = cfg
		colprefix := a.Parameters["_collectionprefix"]
		a.outputs = make([]*btrdb.Stream, len(a.cfg.Outputs))
		for idx, os := range a.cfg.Outputs {
			//Locate or create an output stream
			uu := stableHash(a.Instance, a.Subname, os.ReferenceName)

			s := a.bdb.StreamFromUUID(uu)
			exists, err := s.Exists(context.Background())
			if err != nil {
				lg.Panicf("could not check if stream exists: %v", err)
			}
			if !exists {
				name := os.Name
				unit := os.Unit
				distiller := fmt.Sprintf("%s/%s", a.Instance, a.Subname)
				//create stream and assign to stream
				s, err = a.bdb.Create(context.Background(),
					uu, colprefix+os.Collection, map[string]*string{
						"name":      &name,
						"unit":      &unit,
						"distiller": &distiller}, btrdb.OptKV(os.Annotations))
				if err != nil && btrdb.ToCodedError(err).Code == bte.AmbiguousStream {
					a.Error("creationprefix collides with existing streams")
					//The user has configured a colliding collection prefix for two distillates, lets disambiguate
					rn := rand.Int31()
					newcol := fmt.Sprintf("%s/%s_%s_%s_%08x", colprefix, os.Collection, a.Instance, a.Subname, rn)
					s, err = a.bdb.Create(context.Background(),
						uu, newcol, map[string]*string{
							"name":      &name,
							"unit":      &unit,
							"distiller": &distiller}, btrdb.OptKV(os.Annotations))
					if err != nil {
						lg.Panicf("could not create stream: %v", err)
					}
					//End first create
				} else if err != nil {
					lg.Panicf("could not create stream: %v", err)
				}
			} //end exists
			a.outputs[idx] = s
		} //end range over outputs

		//Enqueue the assignment for execution
		ds.enqueueAssignment(0, a)
	}
	return nil
}

// StartEngine begins processing distillates. It does not return
func (ds *DISTIL) StartEngine() {
	fmt.Printf("active streams is %d\n", ds.activeStreams)
	for i := 0; i < ds.activeStreams; i++ {
		go ds.worker()
	}
	go ds.processLoop()
	for {
		time.Sleep(10 * time.Second)
	}
}

func configWrapper(f func(a *Assignment) *Config, asn *Assignment) (cfg *Config, okay bool) {
	defer func() {
		err := recover()
		if err != nil {
			stack := debug.Stack()
			scanner := bufio.NewScanner(bytes.NewBuffer(stack))
			asn.Error("Assignment config terminal error: %v", err)
			for scanner.Scan() {
				asn.Error("%s", scanner.Text())
			}
			asn.Error("could not complete config: %v", err)
			cfg = nil
			okay = false
			return
		}
	}()
	rv := f(asn)
	return rv, true
}

//Return the underlying BTrDB streams for the assignment inputs
func (asn *Assignment) InputStreams() []*btrdb.Stream {
	return asn.inputs
}

//Return the underyling BTrDB streams for the assignment outputs
func (asn *Assignment) OutputStreams() []*btrdb.Stream {
	return asn.outputs
}

//Try parse a parameter as an integer and abort on failure
func (asn *Assignment) MandatoryIntegerParameter(name string) int64 {
	p, ok := asn.Parameters[name]
	if !ok {
		asn.Abort("missing mandatory parameter %q", name)
	}
	pi, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		asn.Abort("mandatory integer parameter %q failed to parse", name)
	}
	return pi
}

//Try parse a parameter as an float and abort on failure
func (asn *Assignment) MandatoryFloatParameter(name string) float64 {
	p, ok := asn.Parameters[name]
	if !ok {
		asn.Abort("missing mandatory parameter %q", name)
	}
	pf, err := strconv.ParseFloat(p, 64)
	if err != nil {
		asn.Abort("mandatory float parameter %q failed to parse", name)
	}
	return pf
}

//Try parse a parameter as an boolean and abort on failure
func (asn *Assignment) MandatoryBoolParameter(name string) bool {
	p, ok := asn.Parameters[name]
	if !ok {
		asn.Abort("missing mandatory parameter %q", name)
	}
	pf, err := strconv.ParseBool(p)
	if err != nil {
		asn.Abort("mandatory bool parameter %q failed to parse", name)
	}
	return pf
}

//Try parse a parameter as an integer
func (asn *Assignment) OptionalIntegerParameter(name string) (int64, bool) {
	p, ok := asn.Parameters[name]
	if !ok {
		return 0, false
	}
	pi, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return 0, false
	}
	return pi, true
}

//Try parse a parameter as an float
func (asn *Assignment) OptionalFloatParameter(name string) (float64, bool) {
	p, ok := asn.Parameters[name]
	if !ok {
		return 0, false
	}
	pf, err := strconv.ParseFloat(p, 64)
	if err != nil {
		return 0, false
	}
	return pf, true
}

//Try parse a parameter as an boolean
func (asn *Assignment) OptionalBoolParameter(name string) (bool, bool) {
	p, ok := asn.Parameters[name]
	if !ok {
		return false, false
	}
	pf, err := strconv.ParseBool(p)
	if err != nil {
		return false, false
	}
	return pf, true
}

type iterationResult int

const processedData iterationResult = 1
const noData iterationResult = 2
const abort iterationResult = 3

func (asn *Assignment) processIteration() (ir iterationResult, exiterr error) {
	defer func() {
		err := recover()
		if err != nil {
			stack := debug.Stack()
			scanner := bufio.NewScanner(bytes.NewBuffer(stack))
			asn.Error("Assignment terminal error: %v", err)
			for scanner.Scan() {
				asn.Error("%s", scanner.Text())
			}
			asE, ok := err.(error)
			if ok {
				exiterr = asE
			} else {
				asS := err.(string)
				exiterr = errors.New(asS)
			}
			ir = abort
		}
	}()

	//Check for existing cursor
	//	Resume if found
	astate, err := asn.ds.loadassignmentState(asn.ID)
	if err != nil {
		asn.Abort("could not load assignment state: %v\n", err)
		return abort, err
	}
	if astate == nil {
		fmt.Printf("astate nil for %s, constructing\n", asn.ID.String())
		astate = &assignmentState{
			LastVersions:         make([]uint64, len(asn.inputs)),
			LastAlgorithmVersion: asn.cfg.Version,
			TargetVersions:       make([]uint64, len(asn.inputs)),
		}
		for idx := 0; idx < len(asn.inputs); idx++ {
			astate.LastVersions[idx] = 10
		}
	}

	if asn.cfg.Version != astate.LastAlgorithmVersion {
		asn.Info("resetting distillate status due to change in algorithm version")
		//Set the last processed version back to 10
		for idx := 0; idx < len(asn.inputs); idx++ {
			astate.LastVersions[idx] = 10
		}
		astate.InProgress = false
		astate.LastAlgorithmVersion = asn.cfg.Version
	}

	if !astate.InProgress {
		//If the assignment is not in progress, we need to pick target versions
		//Query for the target versions
		for idx, s := range asn.inputs {
			v, err := s.Version(context.Background())
			if err != nil {
				asn.Abort("could not obtain stream version: %v", err)
			}
			astate.TargetVersions[idx] = v
		}
		//Initialize the cursor
		astate.Cursortime = math.MinInt64
		astate.InProgress = true
	}

	//Intersect with the ProcessX limits
	newRange := TimeRange{Start: asn.ProcessAfter, End: asn.ProcessBefore}
	oldRange := TimeRange{Start: astate.LastProcessAfter, End: astate.LastProcessBefore}

	//If there have been no changes, just skip. Don't bother
	//writing the assignment state, we can just regenerate it.
	somechanges := false
	for idx, _ := range asn.inputs {
		if astate.LastVersions[idx] != astate.TargetVersions[idx] {
			somechanges = true
		}
	}
	if !somechanges && newRange == oldRange {
		asn.Info("no changes detected")
		return noData, nil
	}

	//When we compute the time range here, it should be static as it's fed
	//by the assignment state
	//Find the changed ranges
	then := time.Now()

	chranges := make([]TimeRange, 0, 20)
	for idx, in := range asn.inputs {
		//10 is an uncreated stream, we don't need to process that
		if astate.TargetVersions[idx] == 10 {
			return noData, nil
		}
		//We need to do iterative change queries. First we do a high level (1 day) which we
		//expect to require traversing only a few thousand nodes at maximum (eg 10 years is <4k).
		//Then if find the changes fit within a day, we switch to the most accurate changed range possible
		crvCoarse, _, cerr := in.Changes(context.Background(), astate.LastVersions[idx], astate.TargetVersions[idx], 46)
		compressed, err := formChangesList(crvCoarse, cerr)
		if err != nil {
			asn.Abort("could not form changeset list: %v", err)
		}
		if len(compressed) == 1 && compressed[0].Width() <= (1<<46) {
			asn.Info("Adding precise ranges for versions %v to %v", astate.LastVersions[idx], astate.TargetVersions[idx])
			crv, _, cerr := in.Changes(context.Background(), astate.LastVersions[idx], astate.TargetVersions[idx], 0)
			compressed, err = formChangesList(crv, cerr)
			if err != nil {
				asn.Abort("could not form changeset list: %v", err)
			}
		} else {
			asn.Info("Adding coarse ranges for versions %v to %v", astate.LastVersions[idx], astate.TargetVersions[idx])
		}

		chranges = append(chranges, compressed...)
	}

	//Combine the changed ranges across streams to one set of ranges
	merged_ranges := expandPrereqsParallel(chranges)
	lastTime := int64(math.MinInt64)
	if oldRange != newRange {
		asn.Info("detected change in process range")
		removed_ranges := allOfTime.Subtract(newRange)
		added_ranges := newRange.Subtract(oldRange)
		common := oldRange.Intersect(newRange)
		//Remove data in range
		for _, r := range removed_ranges {
			for _, stream := range asn.outputs {
				_, err := stream.DeleteRange(context.Background(), r.Start, r.End)
				if err != nil {
					asn.Abort("could not delete old ranges: %v", err)
				}
			}
		}

		//Fix all changed ranges to be in the common range
		//We will add back the ranges that are in the new range
		//a bit below
		nRanges := make([]TimeRange, 0, len(merged_ranges))
		if common != nil {
			for _, m := range merged_ranges {
				in := m.Intersect(*common)
				if in == nil {
					continue
				}
				nRanges = append(nRanges, *in)
			}
		}
		merged_ranges = nRanges
		//if we have added ranges, intersect them with the full changed ranges
		if len(added_ranges) > 0 {
			chranges_backfill := make([]TimeRange, 0, 20)
			for idx, in := range asn.inputs {
				//We don't need a precise changed range here, this is just a rough filter so we
				//can discard large chunks of the added process ranges. 49 is about 6 days.
				//Any overshoot in here will be filtered down by the Window stages later
				//and we are not overlapping with data we have processed before because
				//we intersect these changes with the added ranges.
				crv, _, cerr := in.Changes(context.Background(), 10, astate.TargetVersions[idx], 49)
				compressed, err := formChangesList(crv, cerr)
				if err != nil {
					asn.Abort("could not form changeset list: %v", err)
				}
				chranges_backfill = append(chranges_backfill, compressed...)
			}
			//Combine the changed ranges across streams to one set of ranges
			backfill_ranges := expandPrereqsParallel(chranges_backfill)
			//Add any new ranges to processing
			hasNew := false
			for _, r := range backfill_ranges {
				for _, added := range added_ranges {
					in := r.Intersect(added)
					if in != nil {
						merged_ranges = append(merged_ranges, *in)
						hasNew = true
					}
				}
			}
			if hasNew {
				//The ranges we added might be adjacent to
				//ranges already in there, sort the list and merge them
				merged_ranges = sortAndMerge(merged_ranges)
			}
		}
	} else {
		//Clamp merged range to new range
		nRanges := make([]TimeRange, 0, len(merged_ranges))
		for _, m := range merged_ranges {
			in := m.Intersect(newRange)
			if in == nil {
				continue
			}
			nRanges = append(nRanges, *in)
		}
		merged_ranges = nRanges
	}
	//Process all ranges that lie after the cursor
	for _, r := range merged_ranges {
		//For log message
		if r.End > lastTime {
			lastTime = r.End
		}
		//Skip this range, we have already done it
		if r.End <= astate.Cursortime {
			fmt.Printf("skipping changed range that appears before the cursor")
			continue
		}
		//If we have done part of this range already, skip the parts we have done
		if r.Start < astate.Cursortime {
			fmt.Printf("adjusting changed range to match cursor")
			r.Start = astate.Cursortime
		}

		//Delete the data in the range, in the outputs
		for _, ostream := range asn.outputs {
			ostream.DeleteRange(context.Background(), r.Start, r.End)
		}

		//The time range is the changed range rounded out to the entry
		//stage window width
		es, ok := asn.cfg.EntryStage.(internalSetupStage)
		if !ok {
			asn.Abort("the first stage must be a window or chunking stage")
		}
		asn.stageSetup(es, r, astate, 0)
	}

	//We completed all of the computation
	for idx, _ := range asn.inputs {
		astate.LastVersions[idx] = astate.TargetVersions[idx]
	}
	astate.LastProcessAfter = asn.ProcessAfter
	astate.LastProcessBefore = asn.ProcessBefore
	astate.InProgress = false
	astate.Cursortime = math.MinInt64
	asn.lazyWriteassignmentState(astate)

	if lastTime == math.MinInt64 {
		asn.Info("FINISHED (took %.2fs) no processing occured (likely due to ProcessBefore/After restrictions)", float64(time.Now().Sub(then)/time.Millisecond)/1000.0)
	} else {
		asn.Info("FINISHED up to %s (took %.2fs)", time.Unix(0, lastTime), float64(time.Now().Sub(then)/time.Millisecond)/1000.0)
	}
	return processedData, nil
}

func (ds *DISTIL) executeAssignment(asn *Assignment) {
	if asn.ua.WorkContext.Err() != nil {
		asn.Error("terminating: context cancel")
		asn.ua.LockContextCancel()
		pmActiveAssignments.Add(-1)
		return
	}

	then := time.Now()
	result, err := asn.processIteration()
	delta := time.Since(then)
	pmAssignmentTime.Observe(float64(delta / time.Microsecond))

	if asn.ua.WorkContext.Err() != nil {
		asn.Error("terminating: context cancel")
		asn.ua.LockContextCancel()
		return
	}

	if err != nil {
		pmInvocationErrors.Add(1)
	}
	if err != nil || result == noData {
		asn.backoff *= 2
		if asn.backoff > MaxBackOff {
			asn.backoff = MaxBackOff
		}
		ds.enqueueAssignment(int64(asn.backoff), asn)
		return
	}
	//We processed a batch of data
	ds.enqueueAssignment(0, asn)
	asn.backoff = MinBackOff
}

func (ds *DISTIL) worker() {
	for {
		a := <-ds.workq
		ds.executeAssignment(a)
		pmAssignmentInvocations.Add(1)
	}
}

func (ds *DISTIL) processLoop() {
	for {
		now := time.Now().Unix()
		qa := ds.peekAssignment()
		if qa == nil || qa.Time > now {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		qa = ds.popAssignment()
		asn := qa.Assignment
		ds.freeAssignment(qa)
		ds.workq <- asn
	}
}

func (ds *DISTIL) enqueueAssignment(delta int64, asn *Assignment) {
	serial := atomic.AddUint64(&ds.aserial, 1)
	ts := time.Now().Unix() + delta
	entry := ds.qapool.Get().(*QueuedAssignment)
	entry.Time = ts
	entry.Serial = serial
	entry.Assignment = asn
	ds.aheapmu.Lock()
	heap.Push(&ds.aheap, entry)
	ds.aheapmu.Unlock()
}

func (ds *DISTIL) peekAssignment() *QueuedAssignment {
	return ds.aheap.Peek()
}

func (ds *DISTIL) popAssignment() *QueuedAssignment {
	return heap.Pop(&ds.aheap).(*QueuedAssignment)
}

func (ds *DISTIL) freeAssignment(q *QueuedAssignment) {
	q.Assignment = nil
	ds.qapool.Put(q)
}

func (ds *DISTIL) connectBDB(apikey string) *btrdb.BTrDB {
	ds.bdbzmu.Lock()
	defer ds.bdbzmu.Unlock()
	bdb, ok := ds.bdbz[apikey]
	if ok {
		return bdb
	}
	bdb, err := btrdb.ConnectAuth(context.Background(), apikey, btrdb.EndpointsFromEnv()...)
	if err != nil {
		lg.Panicf("could not connect to BTrDB: %v", err)
	}
	ds.bdbz[apikey] = bdb
	return bdb
}

func (ds *DISTIL) connect(etcdEp string) {
	lg.Infof("connecting to DISTIL client")
	//Connect to distil client
	ds.dclient = distilclient.NewDistilClient(context.Background(), etcdEp,
		os.Getenv("DISTIL_CLASS"),
		os.Getenv("MY_POD_NAME"))
	ds.etcd = ds.dclient.Etcd()
}

func maxInt64(x int64, y int64) int64 {
	if x > y {
		return x
	}
	return y
}
func minInt64(x int64, y int64) int64 {
	if x < y {
		return x
	}
	return y
}

//Merge a few time ranges, outputting the union of all of them
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

func sortAndMerge(tr []TimeRange) []TimeRange {
	sort.Sort(TimeRangeSlice(tr))
	return mergeAdjacentRanges(tr)
}

const maximumChanges = 100

// If the channel has more than a certain number of changed ranges in it, then
// combine the ranges into a bunch of bigger ranges so that the returned list
// is no more than MaximumChanges long
func formChangesList(vchan chan btrdb.ChangedRange, echan chan error) ([]TimeRange, error) {
	//TODO actually do the limiting
	rv := []TimeRange{}
	for cr := range vchan {
		rv = append(rv, TimeRange{Start: cr.Start, End: cr.End})
	}
	e := <-echan
	if e != nil {
		return nil, e
	}
	return rv, nil
}
