package rowbinary

import (
	"encoding/binary"
)

var UInt64 Type[uint64] = MakeTypeWrapAny[uint64](typeUInt64{})

type typeUInt64 struct{}

func (t typeUInt64) String() string {
	return "UInt64"
}

func (t typeUInt64) Binary() []byte {
	return BinaryTypeUInt64[:]
}

func (t typeUInt64) Write(w Writer, value uint64) error {
	binary.LittleEndian.PutUint64(w.Buffer(), value)
	_, err := w.Write(w.Buffer()[:8])
	return err
}

func (t typeUInt64) Scan(r Reader, v *uint64) error {
	b, err := r.Peek(8)
	if err != nil {
		return err
	}
	*v = binary.LittleEndian.Uint64(b)
	if _, err = r.Discard(8); err != nil {
		return err
	}
	return nil
}
