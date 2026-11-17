package rowbinary

import (
	"fmt"
	"io"
)

func FixedString(length int) Type[[]byte] {
	return MakeTypeWrapAny(typeFixedString{
		length: length,
	})
}

type typeFixedString struct {
	length int
}

func (t typeFixedString) String() string {
	return fmt.Sprintf("FixedString(%d)", t.length)
}

func (t typeFixedString) Binary() []byte {
	return append(BinaryTypeFixedString[:], VarintEncode(uint64(t.length))...)
}

func (t typeFixedString) Write(w Writer, value []byte) error {
	if len(value) != t.length {
		return fmt.Errorf("invalid length %d, expected %d", len(value), t.length)
	}

	_, err := w.Write(value)
	return err
}

func (t typeFixedString) Scan(r Reader, p *[]byte) error {
	*p = (*p)[:0]
	*p = append(*p, make([]byte, t.length)...)

	_, err := io.ReadAtLeast(r, *p, t.length)
	if err != nil {
		return err
	}
	return nil
}
