package rowbinary

import "encoding/binary"

var Int64 Type[int64] = MakeTypeWrapAny[int64](typeInt64{})

type typeInt64 struct{}

func (t typeInt64) String() string {
	return "Int64"
}

func (t typeInt64) Binary() []byte {
	return BinaryTypeInt64[:]
}

func (t typeInt64) Write(w Writer, value int64) error {
	return UInt64.Write(w, uint64(value))
}

func (t typeInt64) Scan(r Reader, v *int64) error {
	b, err := r.Peek(8)
	if err != nil {
		return err
	}
	*v = int64(binary.LittleEndian.Uint64(b))
	if _, err = r.Discard(8); err != nil {
		return err
	}
	return nil
}
