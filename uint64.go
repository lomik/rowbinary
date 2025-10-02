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
	binary.LittleEndian.PutUint64(w.buffer(), value)
	_, err := w.Write(w.buffer()[:8])
	return err
}

func (t typeUInt64) Read(r Reader) (uint64, error) {
	b, err := r.Peek(8)
	if err != nil {
		return 0, err
	}
	ret := binary.LittleEndian.Uint64(b)
	r.Discard(8)
	return ret, nil
}

func (t typeUInt64) Scan(r Reader, v *uint64) (err error) {
	*v, err = t.Read(r)
	return
}
