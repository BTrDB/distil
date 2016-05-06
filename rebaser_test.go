package distil

import (
	"math"
	"testing"

	btrdb "github.com/SoftwareDefinedBuildings/btrdb-go"
)

const NANO int64 = 1000000000
const FREQ int64 = 120
const SPACING_INT64 int64 = NANO / FREQ

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

func makeAligned(ssec, esec int64) []btrdb.StandardValue {
	var s []btrdb.StandardValue = make([]btrdb.StandardValue, 0, (esec - ssec) * 120)
	for second := int64(ssec); second < esec; second++ {
		for i := int64(0); i < 120; i++ {
			t := (second * 1000000000) + (i * SPACING_INT64)
			v := float64(i)
			sv := btrdb.StandardValue{ Time: t, Value: v }
			s = append(s, sv)
		}
	}
	return s
}

func TestFullyAligned(t *testing.T) {
	s := makeAligned(0, 3)
	
	
	var c chan btrdb.StandardValue = sliceToChan(s)
	var pnr Rebaser = RebasePadSnap(FREQ)
	
	var oc chan btrdb.StandardValue = pnr.Process(0 * NANO, 3 * NANO, c)
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

func TestExtraSpace(t *testing.T) {
	s := makeAligned(2, 5)
	
	var c chan btrdb.StandardValue = sliceToChan(s)
	var pnr Rebaser = RebasePadSnap(FREQ)
	
	var oc chan btrdb.StandardValue = pnr.Process(1 * NANO, 6 * NANO, c)
	var os []btrdb.StandardValue = chanToSlice(oc)
	
	var explen int64 = int64(len(s)) + 2 * FREQ
	if int64(len(os)) != explen {
		t.Fatalf("Output has %d points (expected %d)", len(os), explen)
	}
	
	var i int64
	for i = 0; i < FREQ; i++ {
		expt := (1 * NANO) + i * SPACING_INT64
		if os[i].Time != expt || !math.IsNaN(os[i].Value) {
			t.Logf("Got (%d, %f) at entry %d; expected (%d, NaN)", os[i].Time, os[i].Value, i, expt)
			t.Fail()
		}
	}
	for i = 0; i < FREQ; i++ {
		expt := (5 * NANO) + i * SPACING_INT64
		j := i + 4 * FREQ
		if os[j].Time != expt || !math.IsNaN(os[j].Value) {
			t.Logf("Got (%d, %f) at entry %d; expected (%d, NaN)", os[j].Time, os[j].Value, j, expt)
			t.Fail()
		}
	}
	
	for j := range s {
		i := int64(j) + FREQ
		if os[i].Time != s[j].Time || !floatEquals(os[i].Value, s[j].Value) {
			t.Logf("Got (%d, %f) at entry %d; expected (%d, %f)", os[i].Time, os[i].Value, j, s[j].Time, s[j].Value)
			t.Fail()
		}
	}
}
