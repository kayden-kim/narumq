package sample

type first struct{}
type second struct{}

func (first) value() {
	_ = 1
}

func (second) value() {
	_ = 2
}
