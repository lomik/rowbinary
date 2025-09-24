package rowbinary

import (
	"math"

	"github.com/pkg/errors"
)

var Float64 Type[float64] = &typeFloat64{}

type typeFloat64 struct {
}

func (t *typeFloat64) String() string {
	return "Float64"
}

func (t *typeFloat64) Binary() []byte {
	return typeBinaryFloat64[:]
}

func (t *typeFloat64) Write(w Writer, value float64) error {
	return UInt64.Write(w, math.Float64bits(value))
}

func (t *typeFloat64) Read(r Reader) (float64, error) {
	v, err := UInt64.Read(r)
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(v), nil
}

func (t *typeFloat64) WriteAny(w Writer, v any) error {
	value, ok := v.(float64)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeFloat64) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
