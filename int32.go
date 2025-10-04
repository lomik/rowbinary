package rowbinary

import "encoding/binary"

var Int32 Type[int32] = MakeTypeWrapAny[int32](typeInt32{})

type typeInt32 struct{}

func (t typeInt32) String() string {
	return "Int32"
}

func (t typeInt32) Binary() []byte {
	return BinaryTypeInt32[:]
}

func (t typeInt32) Write(w Writer, value int32) error {
	return UInt32.Write(w, uint32(value))
}

func (t typeInt32) Scan(r Reader, v *int32) error {
	b, err := r.Peek(4)
	if err != nil {
		return err
	}
	*v = int32(binary.LittleEndian.Uint32(b))
	if _, err = r.Discard(4); err != nil {
		return err
	}
	return nil
}
