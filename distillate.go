package distil

type Distillate interface {
	Version() int
	LeadNanos() int64
	Rebase() Rebaser
	Process(*InputSet, *OutputSet)
}

type DistillateTools struct {
	DS *DISTIL
}

func (ds *DISTIL) Tools() *DistillateTools {
	return &DistillateTools{ds}
}

func (dd *DistillateTools) Version() int {
	return 1
}
func (dd *DistillateTools) Rebase() Rebaser {
	return RebasePassthrough()
}
func (dd *DistillateTools) LeadNanos() int64 {
	return 0
}
