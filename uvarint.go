package rowbinary

import (
	"bytes"
	"encoding/binary"
)

var UVarint Type[uint64] = MakeTypeWrapAny[uint64](typeUVarint{})

type typeUVarint struct{}

func (t typeUVarint) String() string {
	return "UVarint"
}

func (t typeUVarint) Binary() []byte {
	return BinaryTypeNothing[:]
}

func (t typeUVarint) Write(w Writer, x uint64) error {
	var err error
	i := 0
	for x >= 0x80 {
		if err = w.WriteByte(byte(x) | 0x80); err != nil {
			return err
		}
		x >>= 7
		i++
	}
	if err = w.WriteByte(byte(x)); err != nil {
		return err
	}
	return err
}

func (t typeUVarint) Read(r Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func (t typeUVarint) Scan(r Reader, v *uint64) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}

func UVarintEncode(x uint64) []byte {
	var b bytes.Buffer
	i := 0
	for x >= 0x80 {
		b.WriteByte(byte(x) | 0x80)
		x >>= 7
		i++
	}
	b.WriteByte(byte(x))
	return b.Bytes()
}
