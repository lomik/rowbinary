package rowbinary

import (
	"errors"
)

var Int16 Type[int16] = typeInt16{}
var typeInt16ID = BinaryTypeID(BinaryTypeInt16[:])

type typeInt16 struct{}

func (t typeInt16) String() string {
	return "Int16"
}

func (t typeInt16) Binary() []byte {
	return BinaryTypeInt16[:]
}

func (t typeInt16) ID() uint64 {
	return typeInt16ID
}

func (t typeInt16) Write(w Writer, value int16) error {
	return UInt16.Write(w, uint16(value))
}

func (t typeInt16) Read(r Reader) (int16, error) {
	v, err := UInt16.Read(r)
	return int16(v), err
}

func (t typeInt16) WriteAny(w Writer, v any) error {
	value, ok := v.(int16)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeInt16) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
