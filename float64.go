package rowbinary

import (
	"encoding/binary"
	"math"
)

var Float64 Type[float64] = MakeTypeWrapAny[float64](typeFloat64{})

type typeFloat64 struct{}

func (t typeFloat64) String() string {
	return "Float64"
}

func (t typeFloat64) Binary() []byte {
	return BinaryTypeFloat64[:]
}

func (t typeFloat64) Write(w Writer, value float64) error {
	return UInt64.Write(w, math.Float64bits(value))
}

func (t typeFloat64) Scan(r Reader, v *float64) error {
	b, err := r.Peek(8)
	if err != nil {
		return err
	}
	*v = math.Float64frombits(binary.LittleEndian.Uint64(b))
	if _, err = r.Discard(8); err != nil {
		return err
	}
	return nil
}
