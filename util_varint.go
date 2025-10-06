package rowbinary

import (
	"bytes"
	"encoding/binary"
)

func VarintWrite(w Writer, x uint64) error {
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

func VarintRead(r Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func VarintEncode(x uint64) []byte {
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
