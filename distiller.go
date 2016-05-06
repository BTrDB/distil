package distil

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
