package rowbinary

import "errors"

var _ Type[bool] = customType[bool]{}

type customType[T any] struct {
	origin    Any
	readFunc  func(r Reader) (T, error)
	writeFunc func(w Writer, v T) error
}

func BuildType[T any](origin Any, readFunc func(r Reader) (T, error), writeFunc func(w Writer, v T) error) customType[T] {
	return customType[T]{
		origin:    origin,
		readFunc:  readFunc,
		writeFunc: writeFunc,
	}
}

// Binary implements Type.
func (c customType[T]) Binary() []byte {
	return c.origin.Binary()
}

// ID implements Type.
func (c customType[T]) ID() uint64 {
	return c.origin.ID()
}

// Read implements Type.
func (c customType[T]) Read(r Reader) (T, error) {
	return c.readFunc(r)
}

// ReadAny implements Type.
func (c customType[T]) ReadAny(r Reader) (any, error) {
	return c.Read(r)
}

// String implements Type.
func (c customType[T]) String() string {
	return c.origin.String()
}

// Write implements Type.
func (c customType[T]) Write(w Writer, v T) error {
	return c.writeFunc(w, v)
}

// WriteAny implements Type.
func (c customType[T]) WriteAny(w Writer, v any) error {
	value, ok := v.(T)
	if !ok {
		return errors.New("unexpected type")
	}
	return c.Write(w, value)
}
