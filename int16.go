package rowbinary

import "encoding/binary"

var Int16 Type[int16] = MakeTypeWrapAny[int16](typeInt16{})

type typeInt16 struct{}

func (t typeInt16) String() string {
	return "Int16"
}

func (t typeInt16) Binary() []byte {
	return BinaryTypeInt16[:]
}

func (t typeInt16) Write(w Writer, value int16) error {
	return UInt16.Write(w, uint16(value))
}

func (t typeInt16) Scan(r Reader, v *int16) error {
	b, err := r.Peek(2)
	if err != nil {
		return err
	}
	*v = int16(binary.LittleEndian.Uint16(b))
	if _, err = r.Discard(2); err != nil {
		return err
	}
	return nil
}
