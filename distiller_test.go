package distil

import (
	"testing"
)

func testExpandPrereqsParallel(t *testing.T, input []TimeRange, expoutput []TimeRange) {
	var output = expandPrereqsParallel(input)

	if len(output) != len(expoutput) {
		t.Fatalf("Output has %d intervals (expected %d)", len(output), len(expoutput))
	}

	var i int
	for i = 0; i < len(output); i++ {
		if expoutput[i].start != output[i].start {
			t.Logf("Got [%d, %d) at entry %d; expected [%d, %d)", output[i].start, output[i].end, i, expoutput[i].start, expoutput[i].end)
			t.Fail()
		}
	}
}

func TestAllSeparate(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].start = 0
	input[0].end = 1
	input[1].start = 3
	input[1].end = 7
	input[2].start = 8
	input[2].end = 9
	input[3].start = 10
	input[3].end = 123
	input[4].start = 123456
	input[4].end = 2102031928

	var expoutput = make([]TimeRange, len(input))
	copy(expoutput, input)

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouching(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].start = 0
	input[0].end = 1
	input[1].start = 1
	input[1].end = 4
	input[2].start = 4
	input[2].end = 122
	input[3].start = 122
	input[3].end = 123
	input[4].start = 123
	input[4].end = 2102031928

	var expoutput = make([]TimeRange, 1)
	expoutput[0].start = input[0].start
	expoutput[0].end = input[len(input)-1].end

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouchingSeparate(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].start = 0
	input[0].end = 1
	input[1].start = 1
	input[1].end = 4
	input[2].start = 5
	input[2].end = 122
	input[3].start = 122
	input[3].end = 123
	input[4].start = 123
	input[4].end = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].start = input[0].start
	expoutput[0].end = input[1].end
	expoutput[1].start = input[2].start
	expoutput[1].end = input[len(input)-1].end

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestOverlap(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].start = 0
	input[0].end = 2
	input[1].start = 1
	input[1].end = 4
	input[2].start = 6
	input[2].end = 122
	input[3].start = 120
	input[3].end = 2102031927
	input[4].start = 123
	input[4].end = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].start = input[0].start
	expoutput[0].end = input[1].end
	expoutput[1].start = input[2].start
	expoutput[1].end = input[len(input)-1].end

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouchOverlapSubsume(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].start = 0
	input[0].end = 2
	input[1].start = 0
	input[1].end = 4
	input[2].start = 6
	input[2].end = 120
	input[3].start = 120
	input[3].end = 2102031927
	input[4].start = 123
	input[4].end = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].start = input[0].start
	expoutput[0].end = input[1].end
	expoutput[1].start = input[2].start
	expoutput[1].end = input[len(input)-1].end

	testExpandPrereqsParallel(t, input, expoutput)
}
