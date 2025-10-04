package rowbinary

import (
	"encoding/binary"
	"math"
)

var Float32 Type[float32] = MakeTypeWrapAny[float32](typeFloat32{})

type typeFloat32 struct{}

func (t typeFloat32) String() string {
	return "Float32"
}

func (t typeFloat32) Binary() []byte {
	return BinaryTypeFloat32[:]
}

func (t typeFloat32) Write(w Writer, value float32) error {
	return UInt32.Write(w, math.Float32bits(value))
}

func (t typeFloat32) Scan(r Reader, v *float32) error {
	b, err := r.Peek(4)
	if err != nil {
		return err
	}
	*v = math.Float32frombits(binary.LittleEndian.Uint32(b))
	if _, err = r.Discard(4); err != nil {
		return err
	}
	return nil
}
