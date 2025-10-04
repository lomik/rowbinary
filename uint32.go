package rowbinary

import (
	"encoding/binary"
)

var UInt32 Type[uint32] = MakeTypeWrapAny[uint32](typeUInt32{})

type typeUInt32 struct{}

func (t typeUInt32) String() string {
	return "UInt32"
}

func (t typeUInt32) Binary() []byte {
	return BinaryTypeUInt32[:]
}

func (t typeUInt32) Write(w Writer, v uint32) error {
	binary.LittleEndian.PutUint32(w.buffer(), v)
	_, err := w.Write(w.buffer()[:4])
	return err
}

func (t typeUInt32) Scan(r Reader, v *uint32) (err error) {
	b, err := r.Peek(4)
	if err != nil {
		return err
	}
	*v = binary.LittleEndian.Uint32(b)
	if _, err = r.Discard(4); err != nil {
		return err
	}
	return
}
