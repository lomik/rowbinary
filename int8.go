package rowbinary

import (
	"github.com/pkg/errors"
)

var Int8 Type[int8] = &typeInt8{}

type typeInt8 struct {
}

func (t *typeInt8) String() string {
	return "Int8"
}

func (t *typeInt8) Binary() []byte {
	return typeBinaryInt8[:]
}

func (t *typeInt8) Write(w Writer, value int8) error {
	return UInt8.Write(w, uint8(value))
}

func (t *typeInt8) Read(r Reader) (int8, error) {
	v, err := UInt8.Read(r)
	return int8(v), err
}

func (t *typeInt8) WriteAny(w Writer, v any) error {
	value, ok := v.(int8)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeInt8) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
