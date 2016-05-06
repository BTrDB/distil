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
		if expoutput[i].Start != output[i].Start {
			t.Logf("Got [%d, %d) at entry %d; expected [%d, %d)", output[i].Start, output[i].End, i, expoutput[i].Start, expoutput[i].End)
			t.Fail()
		}
	}
}

func TestAllSeparate(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].Start = 0
	input[0].End = 1
	input[1].Start = 3
	input[1].End = 7
	input[2].Start = 8
	input[2].End = 9
	input[3].Start = 10
	input[3].End = 123
	input[4].Start = 123456
	input[4].End = 2102031928

	var expoutput = make([]TimeRange, len(input))
	copy(expoutput, input)

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouching(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].Start = 0
	input[0].End = 1
	input[1].Start = 1
	input[1].End = 4
	input[2].Start = 4
	input[2].End = 122
	input[3].Start = 122
	input[3].End = 123
	input[4].Start = 123
	input[4].End = 2102031928

	var expoutput = make([]TimeRange, 1)
	expoutput[0].Start = input[0].Start
	expoutput[0].End = input[len(input)-1].End

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouchingSeparate(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].Start = 0
	input[0].End = 1
	input[1].Start = 1
	input[1].End = 4
	input[2].Start = 5
	input[2].End = 122
	input[3].Start = 122
	input[3].End = 123
	input[4].Start = 123
	input[4].End = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].Start = input[0].Start
	expoutput[0].End = input[1].End
	expoutput[1].Start = input[2].Start
	expoutput[1].End = input[len(input)-1].End

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestOverlap(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].Start = 0
	input[0].End = 2
	input[1].Start = 1
	input[1].End = 4
	input[2].Start = 6
	input[2].End = 122
	input[3].Start = 120
	input[3].End = 2102031927
	input[4].Start = 123
	input[4].End = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].Start = input[0].Start
	expoutput[0].End = input[1].End
	expoutput[1].Start = input[2].Start
	expoutput[1].End = input[len(input)-1].End

	testExpandPrereqsParallel(t, input, expoutput)
}

func TestTouchOverlapSubsume(t *testing.T) {
	var input = make([]TimeRange, 5)
	input[0].Start = 0
	input[0].End = 2
	input[1].Start = 0
	input[1].End = 4
	input[2].Start = 6
	input[2].End = 120
	input[3].Start = 120
	input[3].End = 2102031927
	input[4].Start = 123
	input[4].End = 2102031928

	var expoutput = make([]TimeRange, 2)
	expoutput[0].Start = input[0].Start
	expoutput[0].End = input[1].End
	expoutput[1].Start = input[2].Start
	expoutput[1].End = input[len(input)-1].End

	testExpandPrereqsParallel(t, input, expoutput)
}
