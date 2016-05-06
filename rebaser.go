package distil

import "github.com/SoftwareDefinedBuildings/btrdb-go"

// This specifies an input data preprocessor. It may do anything, but is
// typically used for rebasing input streams (removing duplicates and)
// padding missing values
type Rebaser interface {
	Process(start, end int64, input chan btrdb.StandardValue) chan btrdb.StandardValue
}

// Return a rebaser that does not modify input data
func RebasePassthrough() Rebaser {
	return &noRebase{}
}

type noRebase struct{}

func (n *noRebase) Process(start, end int64, input chan btrdb.StandardValue) chan btrdb.StandardValue {
	return input
}

type padSnapRebaser struct {
  freq int
}

func (rb *padSnapRebaser) Process(start, end int64, input chan btrdb.StandardValue) chan btrdb.StandardValue {
  rv := make(chan btrdb.StandardValue, 1000)
  const NANO = 1000000000
  snap := func(T *int64) {
    subsec := T % NANO
    sec := T - subsec
    cycle := (subsec + offset) / period
    if cycle >= rb.freq {
      sec += NANO
      cycle -= rb.freq
    }
    subsec = cycle*period
    T = sec + subsec
  }
  discard := func(c chan btrdb.StandardValue) {
    for _ := range c {
      
    }
  }
  snap(&start)
  snap(&end)

  go func() {
    period := NANO / rb.freq
    offset := period / 2
    expectedTime := start
    for v := range input {
      //First snap the point
      snap(&v.T)

      //Now see if it is before the point we are expecting
      if v.T < expectedTime {
        continue //drop it
      }
      //If it is greater than what we expect, emit until we hit it or the end
      for v.T > expectedTime {
        rv <- btrdb.StandardValue{T:expectedTime, V: NaN}
        expectedTime += period
        snap(&expectedTime)
        if expectedTime >= end {
          close(rv)
          discard(input)
          return
        }
      }
      //If it is what we expect, emit it
      if v.T == expectedTime {
        rv <- v
        expectedTime += period
        snap(&expectedTime)
        if expectedTime >= end {
          close(rv)
          discard(input)
          return
        }
        continue
      }
    }
    //Now we ran out of input. Pad until output
    for expectedTime < end {
      rv <- btrdb.StandardValue{T:expectedTime, V: NaN}
      expectedTime += period
      snap(&expectedTime)
    }
    close(rv)
    discard(input)
    return
  }
  return rv
}
