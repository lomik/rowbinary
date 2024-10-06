package types

import (
	"github.com/pkg/errors"
)

var Int64 Type[int64] = &typeInt64{}

type typeInt64 struct {
}

func (t *typeInt64) String() string {
	return "Int64"
}

func (t *typeInt64) Write(w Writer, value int64) error {
	return UInt64.Write(w, uint64(value))
}

func (t *typeInt64) Read(r Reader) (int64, error) {
	v, err := UInt64.Read(r)
	return int64(v), err
}

func (t *typeInt64) WriteAny(w Writer, v any) error {
	value, ok := v.(int64)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeInt64) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
