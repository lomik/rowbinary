package rowbinary

import (
	"github.com/pkg/errors"
)

var Int32 Type[int32] = &typeInt32{}

type typeInt32 struct {
}

func (t *typeInt32) String() string {
	return "Int32"
}

func (t *typeInt32) Write(w Writer, value int32) error {
	return UInt32.Write(w, uint32(value))
}

func (t *typeInt32) Read(r Reader) (int32, error) {
	v, err := UInt32.Read(r)
	return int32(v), err
}

func (t *typeInt32) WriteAny(w Writer, v any) error {
	value, ok := v.(int32)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeInt32) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
