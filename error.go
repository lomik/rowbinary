package rowbinary

type overflowError struct{}

func (err *overflowError) Error() string {
	return "Overflow error"
}

func newOverflowError() error {
	return &overflowError{}
}

func IsOverflowError(err error) bool {
	_, ok := err.(*overflowError)
	return ok
}
