package rowbinary

import (
	"math"

	"github.com/pkg/errors"
)

var Float32 Type[float32] = &typeFloat32{}

type typeFloat32 struct {
}

func (t *typeFloat32) String() string {
	return "Float32"
}

func (t *typeFloat32) Binary() []byte {
	return typeBinaryFloat32[:]
}

func (t *typeFloat32) Write(w Writer, value float32) error {
	return UInt32.Write(w, math.Float32bits(value))
}

func (t *typeFloat32) Read(r Reader) (float32, error) {
	v, err := UInt32.Read(r)
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(v), nil
}

func (t *typeFloat32) WriteAny(w Writer, v any) error {
	value, ok := v.(float32)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeFloat32) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
