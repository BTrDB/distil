package distil

type QueuedAssignment struct {
	Time       int64
	Serial     uint64
	Assignment *Assignment
}

type AssignmentHeap []*QueuedAssignment

func (h AssignmentHeap) Len() int {
	return len(h)
}
func (h AssignmentHeap) Less(i, j int) bool {
	if h[i].Time < h[j].Time {
		return true
	}
	if h[i].Time > h[j].Time {
		return false
	}
	return h[i].Serial < h[j].Serial
}
func (h AssignmentHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *AssignmentHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(*QueuedAssignment))
}

func (h *AssignmentHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func (h *AssignmentHeap) Peek() *QueuedAssignment {
	if len(*h) == 0 {
		return nil
	}
	return (*h)[0]
}
