package rowbinary

import (
	"errors"
	"fmt"
)

var NotImplementedError = errors.New("not implemented")

type InvalidTypeError struct {
	Msg string
}

type TypeMismatchError struct {
	ExpectedType string
	ActualType   string
}

func (e InvalidTypeError) Error() string {
	return e.Msg
}

func NewInvalidTypeError(msg string) error {
	return InvalidTypeError{Msg: msg}
}

func (e TypeMismatchError) Error() string {
	return fmt.Sprintf("type mismatch: expected %q, got %q", e.ExpectedType, e.ActualType)
}
