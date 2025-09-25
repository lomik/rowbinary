package rowbinary

import (
	"github.com/pkg/errors"
)

var Bool Type[bool] = &typeBool{}

type typeBool struct {
}

func (t *typeBool) String() string {
	return "Bool"
}

func (t *typeBool) Binary() []byte {
	return BinaryTypeBool[:]
}

func (t *typeBool) Write(w Writer, value bool) error {
	if value {
		return UInt8.Write(w, 1)
	}
	return UInt8.Write(w, 0)
}

func (t *typeBool) Read(r Reader) (bool, error) {
	v, err := UInt8.Read(r)
	return v == 1, err
}

func (t *typeBool) WriteAny(w Writer, v any) error {
	value, ok := v.(bool)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeBool) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
