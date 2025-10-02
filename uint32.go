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

func (t typeUInt32) Read(r Reader) (uint32, error) {
	b, err := r.Peek(4)
	if err != nil {
		return 0, err
	}
	ret := binary.LittleEndian.Uint32(b)
	r.Discard(4)
	return ret, nil
}

func (t typeUInt32) Scan(r Reader, v *uint32) (err error) {
	*v, err = t.Read(r)
	return
}
