package distil

import (
	"fmt"
	"math"

	btrdb "github.com/BTrDB/btrdb"
)

//This function can be passed to a window stage for processing aggregate
//data
type WindowProcessFunction func(in *WindowInput, out *WindowOutput) Stage

//This function can be passed to a raw stage for processing raw data
type RawProcessFunction func(in *Input, out *Output) Stage

//This type of function generates the first stage
type SetupProcessFunction func(change TimeRange, out *SetupOutput) Stage

//An output stream that will be found or created
type OutputStream struct {
	//This is used when accessing the output stream by name within the
	//distiller. It is also combined with the Instance/Subname to form
	//the uuid for the output stream
	ReferenceName string
	//This will be appended onto the CollectionPrefix for the distiller
	Collection string
	//The Name tag
	Name string
	//The Unit tag
	Unit string
	//Annotations to add to the stream
	Annotations map[string]string
}

//This represents the configuration of an algorithm
type Config struct {
	//This is where processing will start. A good choice might be
	//distil.StageFilterEmptyChunkedRaw
	EntryStage Stage
	//This is the version of the algorithm. If you increment this,
	//DISTIL will erase all data ever produced by the distillate and
	//re-process it. You should generally only use this in
	//development
	Version uint64
	//These are the output streams. They will be created if they
	//do not exist. Multiple distillates should not write to the
	//same stream, it will cause errors that DISTIL won't be able
	//to detect. The CollectionPrefix configured when the DISTIL
	//instance was created will be prepended onto each of these
	Outputs []OutputStream
}

func (h *Assignment) Info(msg string, args ...interface{}) {
	lg.Infof("["+h.Instance+"/"+h.Subname+"] "+msg, args...)
}
func (h *Assignment) Error(msg string, args ...interface{}) {
	lg.Errorf("["+h.Instance+"/"+h.Subname+"] "+msg, args...)
}
func (h *Assignment) Abort(msg string, args ...interface{}) {
	lg.Panicf("["+h.Instance+"/"+h.Subname+"] "+msg, args...)
}

// This represents a range of time from Start (inclusive) to End (exclusive)
type TimeRange struct {
	Start int64
	End   int64
}

type TimeRangeSlice []TimeRange

// Len is the number of elements in the collection.
func (t TimeRangeSlice) Len() int {
	return len(t)
}

// Less reports whether the element with
// index i should sort before the element with index j.
func (t TimeRangeSlice) Less(i, j int) bool {
	return t[i].Start < t[j].Start
}

// Swap swaps the elements with indexes i and j.
func (t TimeRangeSlice) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *TimeRange) Width() int64 {
	return t.End - t.Start
}

func (t *TimeRange) Subtract(rhs TimeRange) []TimeRange {
	aS := t.Start
	aE := minInt64(rhs.Start, t.End)
	bS := maxInt64(rhs.End, t.Start)
	bE := t.End
	rv := []TimeRange{}
	if aS < aE {
		rv = append(rv, TimeRange{Start: aS, End: aE})
	}
	if bS < bE {
		rv = append(rv, TimeRange{Start: bS, End: bE})
	}
	return rv
}
func (t *TimeRange) Intersect(rhs TimeRange) *TimeRange {
	S := maxInt64(t.Start, rhs.Start)
	E := minInt64(t.End, rhs.End)
	if E <= S {
		return nil
	}
	return &TimeRange{Start: S, End: E}
}

//This represents the input data to a WindowProcessFunction
type WindowInput struct {
	asn         *Assignment
	startIndex  int
	endIndex    int
	samples     [][]btrdb.StatPoint
	tr          TimeRange
	windowWidth int64
}

//How many streams are there
func (i *WindowInput) NumStreams() int {
	return len(i.samples)
}

//How big is each window. Every stream will have the same size windows
func (i *WindowInput) WindowWidth() int64 {
	return i.windowWidth
}

//Get a the stats for a specific window index. Windows between
//0 and NumSamples are within the changed range. Indices out of that
//range are as a result of leading/trailing nanos. Stream is an index
//into the array of input streams assigned by the selector
func (i *WindowInput) Get(stream int, sample int) btrdb.StatPoint {
	if stream < 0 || stream >= len(i.samples) {
		i.asn.Abort("Distillate attempted to read data to from an input stream that does not exist (access=%d, available=%d)", stream, len(i.samples))
	}
	realSample := sample + i.startIndex
	if realSample < 0 || realSample >= len(i.samples[stream]) {
		i.asn.Abort("Distillate attempted to access sample outside Window InputSet. stream=%d sample=%d realsample=%d available=%d", stream, sample, realSample, len(i.samples[stream]))
	}
	return i.samples[stream][realSample]
}

//Get the time range that corresponds with the given window index
func (i *WindowInput) GetSampleRange(sample int) TimeRange {
	realSample := sample + i.startIndex
	if realSample < 0 || realSample >= len(i.samples[0]) {
		i.asn.Abort("Distillate attempted to access sample outside the Window InputSet sample=%d realsample=%d", sample, realSample)
	}
	return TimeRange{Start: i.samples[0][realSample].Time, End: i.samples[0][realSample].Time + i.windowWidth}
}

//Returns the last index of the changed data
func (i *WindowInput) NumSamples() int {
	return i.endIndex
}

//Returns the final loaded index, including trailing data
func (i *WindowInput) TrailingEndIndex() int {
	return len(i.samples[0]) - i.startIndex
}

//Returns the first loaded index, which is negative if there
//are leading nanoseconds
func (i *WindowInput) LeadingStartIndex() int {
	return -i.startIndex
}

//Get the time range that has changed that is being processed. This does not include
//lead time
func (i *WindowInput) GetRange() TimeRange {
	return i.tr
}

//Input represents the set of data passed to a raw process function
type Input struct {
	asn          *Assignment
	startIndexes []int
	endIndexes   []int
	samples      [][]btrdb.RawPoint
	tr           TimeRange
}

//How many streams are present
func (i *Input) NumStreams() int {
	return len(i.samples)
}

//Get a data point from the InputSet, stream is an index into the array
//of input streams assigned by the selector. Sample is the index.
//0 is the first sample in the changed range. Negative indices are lead samples
//(see LeadNanos) that can be used for context and samples past NumSamples
//are trailing samples (see TrailNanos)
func (is *Input) Get(stream int, sample int) btrdb.RawPoint {
	if stream < 0 || stream >= len(is.samples) {
		is.asn.Abort("Distillate attempted to read data to from an input stream that does not exist")
	}
	realSample := sample + is.startIndexes[stream]
	if realSample < 0 || realSample >= len(is.samples[stream]) {
		is.asn.Abort("Distillate attempted to access sample outside the InputSet stream=%d sample=%d realsample=%d", stream, sample, realSample)
	}
	return is.samples[stream][realSample]
}

//Get the number of positive samples in the given stream that lie within
//the changed range
func (is *Input) NumSamples(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		is.asn.Abort("Distillate attempted to read data to from an input stream that does not exist")
	}
	return len(is.samples[stream]) - is.startIndexes[stream]
}

//Get the number of negative samples (lead samples) in the given stream
func (is *Input) LeadingStartIndex(stream int) int {
	if stream < 0 || stream >= len(is.samples) {
		panic(fmt.Sprintf("Distillate attempted to access stream outside InputSet: %d", stream))
	}
	return -is.startIndexes[stream]
}

//Get the index of the last loaded sample, including trailing nanos
func (is *Input) TrailingEndIndex(stream int) int {
	return len(is.samples[0]) - is.startIndexes[stream]
}

//Get the time range that has changed that is being processed. This does not include
//lead time
func (is *Input) GetRange() TimeRange {
	return is.tr
}

//A setup stage outputs the time ranges that need to be processed
//by a window stage
type SetupOutput struct {
	asn            *Assignment
	nextStageRange TimeRange
}

//A setup stage filters the ranges of time that are processed by the next
//stage. To include a range of time, call this function. If this is not called
//then no data will be processed by the next stage. This can also be used
//to ensure the processing ranges are of a particular size
func (sout *SetupOutput) SetNextStageRange(tr TimeRange) {
	sout.nextStageRange = tr
}

//Represents the set of data emitted by a window processing stage
type WindowOutput struct {
	asn             *Assignment
	nextStageRanges []TimeRange
	outbufs         [][]btrdb.RawPoint
	r               TimeRange
}

//A window stage filters the ranges of time that are processed by the next
//stage. To include a range of time, call this function. If this is not called
//then no data will be processed by the next stage
func (wout *WindowOutput) AddNextStageRange(tr TimeRange) {
	wout.nextStageRanges = append(wout.nextStageRanges, tr)
}

//Add a point to the given stream
func (wout *WindowOutput) AddPoint(stream int, p btrdb.RawPoint) {
	if stream < 0 || stream >= len(wout.outbufs) {
		wout.asn.Abort("Distillate attempted to write data to an output stream that does not exist")
	}
	if p.Time < wout.r.Start || p.Time >= wout.r.End {
		wout.asn.Abort("Distillate attempted to write data outside of the changed time range")
	}
	//This is acceptable, it simplifies the algorithm writing
	if math.IsNaN(p.Value) {
		return
	}
	wout.outbufs[stream] = append(wout.outbufs[stream], p)
}

//A utility function, this constructs a point and calls AddPoint
func (wout *WindowOutput) Add(stream int, time int64, val float64) {
	wout.AddPoint(stream, btrdb.RawPoint{Time: time, Value: val})
}

// OutputSet is a handle onto the output streams and is used for writing back data
// from processing
type Output struct {
	asn     *Assignment
	outbufs [][]btrdb.RawPoint
	r       TimeRange
}

// Add a point to the given stream
func (oss *Output) AddPoint(stream int, p btrdb.RawPoint) {
	if stream < 0 || stream >= len(oss.outbufs) {
		oss.asn.Abort("Distillate attempted to write data to an output stream that does not exist")
	}
	if p.Time < oss.r.Start || p.Time >= oss.r.End {
		oss.asn.Abort("Distillate attempted to write data outside of the changed time range")
	}
	//This is acceptable, it simplifies the algorithm writing
	if math.IsNaN(p.Value) {
		return
	}
	oss.outbufs[stream] = append(oss.outbufs[stream], p)
}

// A utility function, this constructs a point and calls AddPoint
func (oss *Output) Add(stream int, time int64, val float64) {
	oss.AddPoint(stream, btrdb.RawPoint{Time: time, Value: val})
}
