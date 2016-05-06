package distil

// Distillate is the interface that all algorithms must implement in order for
// them to be registerable with the DISTIL engine. DistillateTools can be used
// to obtain default implementations
type Distillate interface {
	// Get the version of the algorithm implemented by this distillate.
	// Future revisions of the DISTIL engine will allow for recalculation
	// of the entire stream when the Version() changes.
	Version() int
	// Get the number of nanoseconds that should be loaded ahead of the
	// time range that has changed, available as negative sample indices
	// in Process()
	LeadNanos() int64
	// Get the Rebase stage for this distillate
	Rebase() Rebaser
	// This is used by the Engine to configure a handle for your
	// distillate to access DISTIL. The handle can be used any time
	// after registration with the DS field:
	//
	//    var handle *distil.DISTIL = myDistillate.DS
	//
	SetEngine(ds *DISTIL)
	// This is called once per changed range to compute the output data
	// corresponding to the changes in the input data
	Process(*InputSet, *OutputSet)
}

// DistillateTools is intended to provide default implementations of the
// Distillate interface, and is included in a Distillate like so:
//
//   type MyDistillate struct {
//     distil.DistillateTools
//     //your additional fields
//   }
//
type DistillateTools struct {
	DS *DISTIL
}

func (dd *DistillateTools) SetEngine(ds *DISTIL) {
	dd.DS = ds
}

// Default Version() that returns 1
func (dd *DistillateTools) Version() int {
	return 1
}

// Default Rebase() which does not modify the data
func (dd *DistillateTools) Rebase() Rebaser {
	return RebasePassthrough()
}

// Default LeadNanos() that returns 0
func (dd *DistillateTools) LeadNanos() int64 {
	return 0
}
