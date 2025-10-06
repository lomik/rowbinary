package rowbinary

import (
	"errors"
)

var NotImplementedError = errors.New("not implemented")

type InvalidTypeError struct {
	Msg string
}

func (e InvalidTypeError) Error() string {
	return e.Msg
}

func NewInvalidTypeError(msg string) error {
	return InvalidTypeError{Msg: msg}
}
