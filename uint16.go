package rowbinary

import (
	"encoding/binary"
)

var UInt16 Type[uint16] = MakeTypeWrapAny[uint16](typeUInt16{})

type typeUInt16 struct{}

func (t typeUInt16) String() string {
	return "UInt16"
}

func (t typeUInt16) Binary() []byte {
	return BinaryTypeUInt16[:]
}

func (t typeUInt16) Write(w Writer, v uint16) error {
	binary.LittleEndian.PutUint16(w.buffer(), v)
	_, err := w.Write(w.buffer()[:2])
	return err
}

func (t typeUInt16) Scan(r Reader, v *uint16) error {
	b, err := r.Peek(2)
	if err != nil {
		return err
	}
	*v = binary.LittleEndian.Uint16(b)
	if _, err = r.Discard(2); err != nil {
		return err
	}
	return nil
}
