package distil

import (
	"context"
	"fmt"
	"time"

	btrdb "github.com/BTrDB/btrdb"
)

//How many windows we will query at once
const maxWindowsPerInvocation = 8192

//A stage is a set of computation done on a range of time. A stage
//can filter the portions of that range of time that subsequent stages
//execute on, and can choose the parameters for the next stage
type Stage interface {
	//The number of nanoseconds required before and after each time range
	//that gets processed (present as indices < 0 or >= NumSamples)
	ExtraNanos() (int64, int64)
}

//Stages that require window data implement this interface
type internalWindowStage interface {
	Stage
	//How wide each window will be
	WindowWidth() int64
	//The callback to process the data
	WindowProcessFunction() WindowProcessFunction
	//If nonzero, each time range will be snapped to power of two.
	//Should be used with a power of two WindowWidth
	PointWidth() int
}

type internalSetupStage interface {
	Stage
	SetupProcessFunction() SetupProcessFunction
}

//Stages that require raw data implement this interface
type internalRawStage interface {
	Stage
	//The rebaser that transforms the underlying raw data
	Rebaser() Rebaser
	//The callback to process the data
	RawProcessFunction() RawProcessFunction
}

//A generic setup stage
type setupStage struct {
	procFunc SetupProcessFunction
}

func (s *setupStage) SetupProcessFunction() SetupProcessFunction {
	return s.procFunc
}
func (s *setupStage) ExtraNanos() (int64, int64) {
	return 0, 0
}

var _ internalSetupStage = &setupStage{}

//A generic window data stage
type windowStage struct {
	leadNanos   int64
	trailNanos  int64
	windowWidth int64
	pointWidth  int
	procFunc    WindowProcessFunction
}

func (s *windowStage) WindowProcessFunction() WindowProcessFunction {
	return s.procFunc
}
func (s *windowStage) ExtraNanos() (int64, int64) {
	return s.leadNanos, s.trailNanos
}
func (s *windowStage) WindowWidth() int64 {
	return s.windowWidth
}
func (s *windowStage) PointWidth() int {
	return s.pointWidth
}

//Ensure windowStage implements the interface
var _ internalWindowStage = &windowStage{}

//A window-processing stage that uses power-of-two aligned windows for
//efficiency. PW is the log_2 of the window width in nanosecond, for
//example 9 means 2^9 nanoseconds per window. The changed time ranges
//will be snapped to the corresponding power of two boundaries
func StageAlignedWindow(pw int, f WindowProcessFunction) Stage {
	return StageAlignedWindowExtraNanos(pw, f, 0, 0)
}

//A StageAlignedWindow with leading and trailing nanoseconds. These manifest
//as indices <0 or >= NumSamples
func StageAlignedWindowExtraNanos(pw int, f WindowProcessFunction, leadNanos, trailNanos int64) Stage {
	if leadNanos < 0 || trailNanos < 0 {
		panic("lead/trail nanos must be positive")
	}
	if pw < 0 || pw >= 64 {
		panic("bad pointwidth")
	}
	return &windowStage{
		windowWidth: 1 << uint(pw),
		procFunc:    f,
		leadNanos:   leadNanos,
		trailNanos:  trailNanos,
		pointWidth:  pw,
	}
}

//A window-processing stage that uses arbitrary sized windows that
//start at the beginning of the changed ranges, no alignment occurs
func StageWindow(width int64, f WindowProcessFunction) Stage {
	return StageWindowExtraNanos(width, f, 0, 0)
}

//A StageWindow with leading and trailing nanoseconds. These manifest
//as indices <0 or >= NumSamples
func StageWindowExtraNanos(width int64, f WindowProcessFunction, leadNanos, trailNanos int64) Stage {
	if leadNanos < 0 || trailNanos < 0 {
		panic("lead/trail nanos must be positive")
	}
	if width < 0 {
		panic("width must be positive")
	}
	return &windowStage{
		windowWidth: width,
		procFunc:    f,
		leadNanos:   leadNanos,
		trailNanos:  trailNanos,
	}
}

func StageSetup(f SetupProcessFunction) Stage {
	return &setupStage{
		procFunc: f,
	}
}

func StageFilterEmptyChunkedRaw(raw Stage) Stage {
	return &filterEmptyChunkedRawStage{
		raw: raw,
	}
}

//A chunk stage is like a window stage but prepares for a raw stage.
//It has its own process function.
type chunkStage struct {
	next       Stage
	filterZero bool
}

func (s *chunkStage) WindowProcessFunction() WindowProcessFunction {
	return s.chunkRaw
}
func (s *chunkStage) ExtraNanos() (int64, int64) {
	return 0, 0
}
func (s *chunkStage) WindowWidth() int64 {
	//About 9 minutes
	return 1 << 39
}
func (s *chunkStage) PointWidth() int {
	return 39
}

//Ensure chunk stage implements the window stage interface
var _ internalWindowStage = &chunkStage{}

//A stage that prepares a large changed range for processing by a raw
//stage. In general this should ALWAYS be used before a raw stage
//unless you specifically ensure that your last window stage emits
//ranges of an appropriate size for processing (e.g that they fit in memory)
//This also does not process ranges of time where any of the input
//streams is missing data. If you need to still process those ranges
//use StageChunkRawIncludeMissing
func StageChunkRaw(raw Stage) Stage {
	return &chunkStage{
		next:       raw,
		filterZero: true,
	}
}

//A stage that prepares a large changed range for processing by a raw
//stage. In general this should ALWAYS be used before a raw stage
//unless you specifically ensure that your last window stage emits
//ranges of an appropriate size for processing (e.g that they fit in memory)
func StageChunkRawIncludeMissing(raw Stage) Stage {
	return &chunkStage{
		next:       raw,
		filterZero: false,
	}
}

//A generic raw data stage
type rawStage struct {
	rebaser    Rebaser
	leadNanos  int64
	trailNanos int64
	procFunc   RawProcessFunction
}

func (s *rawStage) WindowProcessFunction() WindowProcessFunction {
	return nil
}
func (s *rawStage) RawProcessFunction() RawProcessFunction {
	return s.procFunc
}
func (s *rawStage) ExtraNanos() (int64, int64) {
	return s.leadNanos, s.trailNanos
}
func (s *rawStage) Rebaser() Rebaser {
	return s.rebaser
}

var _ internalRawStage = &rawStage{}

//A raw stage applies the given function over the data, pre-transforming
//it with the given rebaser. At present this must be the last stage
//in the pipeline (there can be no further stages)
func StageRaw(rebase Rebaser, f RawProcessFunction) Stage {
	return StageRawExtraNanos(rebase, f, 0, 0)
}

//A RawStage that fetches extra data before and after each time range. These
//manifest as indices <0 or >= NumSamples
func StageRawExtraNanos(rebase Rebaser, f RawProcessFunction, leadNanos, trailNanos int64) Stage {
	if rebase == nil {
		rebase = RebasePassthrough()
	}
	return &rawStage{
		rebaser:    rebase,
		leadNanos:  leadNanos,
		trailNanos: trailNanos,
		procFunc:   f,
	}
}

//A euphamism for 'nil' the last stage.
func StageDone() Stage {
	return nil
}

//This processes a raw stage and will update the assignment state after each
//process function invocation
func (asn *Assignment) stageRaw(s internalRawStage, tr TimeRange, cr TimeRange, astate *assignmentState, depth int) {
	if depth > MaxStages {
		asn.Abort("hit maximum number of stages")
	}
	if tr.Start < cr.Start {
		tr.Start = cr.Start
	}
	if tr.End > cr.End {
		tr.End = cr.End
	}
	//It is possible, if the tr is small enough, that we just entirely removed the
	//tr by adjusting it to match the cr
	if tr.End <= tr.Start {
		//Just do nothing
		return
	}
	lead, trail := s.ExtraNanos()
	realstart := tr.Start - lead
	realend := tr.End + trail

	iset := &Input{
		asn:          asn,
		samples:      make([][]btrdb.RawPoint, len(asn.inputs)),
		startIndexes: make([]int, len(asn.inputs)),
		endIndexes:   make([]int, len(asn.inputs)),
	}
	for idx, stream := range asn.inputs {
		underlyingChannel, _, cerr := stream.RawValues(context.Background(), realstart, realend, astate.TargetVersions[idx])
		processedChannel := s.Rebaser().ProcessRaw(realstart, realend, underlyingChannel)
		iset.samples[idx] = make([]btrdb.RawPoint, 0, 32768)
		startidx := 0
		endidx := 0
		for p := range processedChannel {
			//Count the number of points before start
			if p.Time < tr.Start {
				startidx++
			}
			//Count the number of points before end
			if p.Time < tr.End {
				endidx++
			}
			iset.samples[idx] = append(iset.samples[idx], p)
		}
		if err := <-cerr; err != nil {
			asn.Abort("could not stream raw values: %v", err)
		}
		iset.startIndexes[idx] = startidx
		iset.endIndexes[idx] = endidx
		pmProcessedPoints.Add(float64(len(iset.samples[idx])))
	}

	oset := &Output{
		asn:     asn,
		outbufs: make([][]btrdb.RawPoint, len(asn.outputs)),
		r:       tr,
	}
	then := time.Now()
	nextStage := s.RawProcessFunction()(iset, oset)
	for idx, stream := range asn.outputs {
		err := stream.InsertF(context.Background(), len(oset.outbufs[idx]),
			func(i int) int64 { return oset.outbufs[idx][i].Time },
			func(i int) float64 { return oset.outbufs[idx][i].Value },
		)
		if err != nil {
			asn.Abort("could not insert range: %v\n", err)
		}
		pmInsertedPoints.Add(float64(len(oset.outbufs[idx])))
	}
	delta := time.Since(then)
	pmRawStageTime.Observe(float64(delta / time.Microsecond))
	pmRawStageTimeRatio.Observe(float64(tr.Width()/1e3) / float64(delta/time.Microsecond))

	if nextStage == nil {
		//Update the astate
		astate.Cursortime = tr.End
		asn.lazyWriteassignmentState(astate)
		return
	} else {
		lg.Panicf("having a stage following raw is not supported")
	}
}

func (asn *Assignment) stageSetup(s internalSetupStage, cr TimeRange, astate *assignmentState, depth int) {
	fmt.Printf("stage setup called: tr=%v\n", cr)
	so := &SetupOutput{asn: asn}
	so.nextStageRange = cr
	nextStage := s.SetupProcessFunction()(cr, so)
	switch v := nextStage.(type) {
	case internalWindowStage:
		asn.stageWindow(v, so.nextStageRange, cr, astate, depth+1)
	case internalRawStage:
		asn.stageRaw(v, so.nextStageRange, cr, astate, depth+1)
	default:
		asn.Abort("unexpected stage type")
	}

}

func roundToPW(x int64, pw uint8) int64 {
	return ((x - btrdb.MinimumTime) & ^((1 << pw) - 1)) + btrdb.MinimumTime
}

func needsPWRounding(x int64, pw uint8) bool {
	return ((x - btrdb.MinimumTime) & ^((1 << pw) - 1)) != 0
}

//This processes window stages. In the event that a window stage is the last in the chain, it will also
//update the assignment state
func (asn *Assignment) stageWindow(s internalWindowStage, tr TimeRange, cr TimeRange, astate *assignmentState, depth int) {
	fmt.Printf("stage window called: tr=%v\n", tr)
	if depth > MaxStages {
		asn.Abort("hit maximum number of stages")
	}
	//expand tr to line up with pointWidth, this is noop if zero
	tr.Start = roundToPW(tr.Start, uint8(s.PointWidth()))
	if needsPWRounding(tr.End, uint8(s.PointWidth())) {
		tr.End = roundToPW(tr.End, uint8(s.PointWidth()))
		tr.End += s.WindowWidth()
	}

	//This can happen, especially with default ProcessBefore
	for tr.End >= btrdb.MaximumTime {
		tr.End -= s.WindowWidth()
	}
	numwindows := int((tr.End - tr.Start) / s.WindowWidth())
	leadnanos, trailnanos := s.ExtraNanos()

	//Round up to the nearest window width on lead and trailing nanos
	if leadnanos%s.WindowWidth() != 0 {
		leadnanos = ((leadnanos / s.WindowWidth()) + 1) * s.WindowWidth()
	}
	if trailnanos%s.WindowWidth() != 0 {
		trailnanos = ((trailnanos / s.WindowWidth()) + 1) * s.WindowWidth()
	}

	//Tr is the time range from the parent that might actually be wider than
	//it needs to be (because it's the same as a parent window that was larger).
	//If possible, narrow it down
	if tr.Start < cr.Start {
		delta := cr.Start - tr.Start
		nwindows := delta / s.WindowWidth()
		tr.Start += nwindows * s.WindowWidth()
	}
	if tr.End > cr.End {
		delta := tr.End - cr.End
		nwindows := delta / s.WindowWidth()
		tr.End -= nwindows * s.WindowWidth()
	}

	//Break up the windows into chunks of MaxWindowsPerInvocation
	for i := 0; i < numwindows; i += maxWindowsPerInvocation {
		realstart := tr.Start + int64(i)*int64(s.WindowWidth())
		realend := tr.Start + (int64(i)+maxWindowsPerInvocation)*s.WindowWidth()
		if realend > tr.End {
			realend = tr.End
		}

		iset := &WindowInput{
			asn:         asn,
			startIndex:  int(leadnanos / s.WindowWidth()),
			windowWidth: s.WindowWidth(),
			samples:     make([][]btrdb.StatPoint, len(asn.inputs)),
			tr:          tr,
		}
		iset.endIndex = iset.startIndex + int((realend-realstart)/s.WindowWidth())

		realstart -= leadnanos
		realend += trailnanos
		expectedWindows := (realend - realstart) / s.WindowWidth()

		for idx, stream := range asn.inputs {
			cwindows, _, cerr := stream.Windows(context.Background(), realstart, realend, uint64(s.WindowWidth()), 0, astate.TargetVersions[idx])
			iset.samples[idx] = make([]btrdb.StatPoint, 0, expectedWindows)
			for w := range cwindows {
				iset.samples[idx] = append(iset.samples[idx], w)
			}
			if len(iset.samples[idx]) != int(expectedWindows) {
				asn.Info("window count did not match. realstart=%d realend=%d ww=%d, got=%d exp=%d", realstart, realend, s.WindowWidth(), len(iset.samples[idx]), expectedWindows)
			}
			if err := <-cerr; err != nil {
				asn.Abort("error obtaining windows: %v", err)
			}
		}
		for _, sz := range iset.samples {
			if iset.endIndex > len(sz) {
				asn.Info("end index did not match samples: ei=%d, len(sz)=%d", iset.endIndex, len(sz))
			}
		}

		oset := &WindowOutput{
			asn:     asn,
			outbufs: make([][]btrdb.RawPoint, len(asn.outputs)),
			r:       tr,
		}
		nextStage := s.WindowProcessFunction()(iset, oset)

		for idx, stream := range asn.outputs {
			if len(oset.outbufs[idx]) > 0 {
				err := stream.InsertF(context.Background(), len(oset.outbufs[idx]),
					func(i int) int64 { return oset.outbufs[idx][i].Time },
					func(i int) float64 { return oset.outbufs[idx][i].Value },
				)
				if err != nil {
					asn.Abort("could not insert range: %v\n", err)
				}
			}
		}

		if nextStage == nil {
			lg.Infof("next stage nil")
			//Update the astate
			astate.Cursortime = tr.End
			asn.lazyWriteassignmentState(astate)
			return
		}
		if es, ok := nextStage.(internalWindowStage); ok {
			nextRanges := mergeAdjacentRanges(oset.nextStageRanges)
			for _, r := range nextRanges {
				asn.stageWindow(es, r, cr, astate, depth+1)
			}
		} else {
			//Note that we don't merge the ranges. The job of the second to last stage
			//(the last stat stage) is to output ranges that are appropriately sized
			//for raw computation in one go
			rs := nextStage.(internalRawStage)
			for _, r := range oset.nextStageRanges {
				asn.stageRaw(rs, r, cr, astate, depth+1)
			}
		}
	}
}

//This function joins all adjacent time ranges in the list so as to
//reduce the total number of elements in the list while still covering
//the same amount of time.
func mergeAdjacentRanges(in []TimeRange) []TimeRange {
	if len(in) <= 1 {
		return in
	}
	rv := make([]TimeRange, 0, len(in)/2)
	e := in[0]
	for _, n := range in[1:] {
		if n.Start == e.End {
			e.End = n.End
		} else {
			rv = append(rv, e)
			e = n
		}
	}
	rv = append(rv, e)
	return rv
}

//chunkRaw breaks up the given windows based on their density
//and produces output windows appropriate for a raw stage
func (s *chunkStage) chunkRaw(in *WindowInput, out *WindowOutput) Stage {
	fmt.Printf("chunk raw called\n")
	var optimalBatchSize uint64 = 100000

	for i := 0; i < in.NumSamples(); i++ {

		zero := false
		var totalPts uint64
		for si := 0; si < in.NumStreams(); si++ {
			w := in.Get(si, i)
			if s.filterZero && w.Count == 0 {
				zero = true
				break
			}
			pts := s.next.(internalRawStage).Rebaser().EstimatePoints(w.Time, w.Time+int64(in.WindowWidth()), w.Count)
			totalPts += pts
		}
		//Only set if filterZero is true
		if zero {
			continue
		}

		//Although we want to try ensure the total number of points is
		//near optimalBatchSize, we also don't want to load less than
		//1000 points per stream as the overhead will be too great
		pointsPerPiece := optimalBatchSize / uint64(in.NumStreams())
		if pointsPerPiece < 1000 {
			pointsPerPiece = 1000
		}
		pieces := int((totalPts / pointsPerPiece) + 1)
		nanosPerPiece := in.WindowWidth() / int64(pieces)
		cursor := in.Get(0, i).Time
		for k := 0; k < pieces; k++ {
			end := cursor + int64(nanosPerPiece)
			if k == pieces-1 {
				end = in.Get(0, i).Time + int64(in.WindowWidth())
			}
			out.AddNextStageRange(TimeRange{
				Start: cursor,
				End:   end,
			})
			cursor += int64(nanosPerPiece)
		}
	}
	return s.next
}

type filterEmptyChunkedRawStage struct {
	raw Stage
}

func (s *filterEmptyChunkedRawStage) SetupProcessFunction() SetupProcessFunction {
	return s.proc
}
func (s *filterEmptyChunkedRawStage) ExtraNanos() (int64, int64) {
	return 0, 0
}
func (s *filterEmptyChunkedRawStage) proc(cr TimeRange, out *SetupOutput) Stage {
	out.SetNextStageRange(cr)
	//If cr is large (> 50 days), pre-filter it so we don't materialize
	//a ton of small windows in the chunked raw stage
	if cr.End-cr.Start > 50*24*60*60*1e9 {
		//TODO what is 49
		return StageAlignedWindow(49, s.windowFilter)
	}
	//<50 days of pw=39 windows is acceptable
	return StageChunkRaw(s.raw)
}
func (s *filterEmptyChunkedRawStage) windowFilter(in *WindowInput, out *WindowOutput) Stage {
	for i := 0; i < in.NumSamples(); i++ {
		zero := false
		for s := 0; s < in.NumStreams(); s++ {
			w := in.Get(s, i)
			if w.Count == 0 {
				zero = true
				break
			}
		}
		if !zero {
			out.AddNextStageRange(in.GetSampleRange(i))
		}
	}
	return StageChunkRaw(s.raw)
}
