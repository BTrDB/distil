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
