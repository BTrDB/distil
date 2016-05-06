package distil

import (
	"math"
	"testing"

	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
)

const SPACING_INT64 int64 = 1000000000 / 120
const FREQ int64 = 120

func sliceToChan(s []btrdb.StandardValue) chan btrdb.StandardValue {
	var rv chan btrdb.StandardValue = make(chan btrdb.StandardValue)
	
	go func () {
		for _, sv := range s {
			rv <- sv
		}
		close(rv)
	}()
	
	return rv
}

func chanToSlice(svs chan btrdb.StandardValue) []btrdb.StandardValue {
	var rv = make([]btrdb.StandardValue, 0, 8)
	for sv := range svs {
		rv = append(rv, sv)
	}
	return rv
}

func floatEquals(x float64, y float64) bool {
	return math.Abs(x - y) <= 1e-10 * math.Max(math.Abs(x), math.Abs(y))
}

func TestFullyAligned(t *testing.T) {
	var s []btrdb.StandardValue = make([]btrdb.StandardValue, 0, 360)
	for second := int64(0); second < 3; second++ {
		for i := int64(0); i < 120; i++ {
			t := (second * 1000000000) + (i * SPACING_INT64)
			v := float64(i)
			sv := btrdb.StandardValue{ Time: t, Value: v }
			s = append(s, sv)
		}
	}
	
	var c chan btrdb.StandardValue = sliceToChan(s)
	var pnr Rebaser = RebasePadSnap(FREQ)
	
	var oc chan btrdb.StandardValue = pnr.Process(0, 3000000000, c)
	var os []btrdb.StandardValue = chanToSlice(oc)
	
	if len(os) != len(s) {
		t.Fatalf("Output has %d points (expected %d)", len(os), len(s))
	}
	
	for i := range os {
		if os[i].Time != s[i].Time || !floatEquals(os[i].Value, s[i].Value) {
			t.Logf("Got (%d, %f) at entry %d; expected (%d, %f)", os[i].Time, os[i].Value, i, s[i].Time, s[i].Value)
			t.Fail()
		}
	}
}
