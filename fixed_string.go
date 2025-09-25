package rowbinary

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
)

var _ Type[[]byte] = FixedString(10)

func FixedString(length int) *typeFixedString {
	return &typeFixedString{
		length: length,
	}
}

type typeFixedString struct {
	length int
}

func (t *typeFixedString) String() string {
	return fmt.Sprintf("FixedString(%d)", t.length)
}

func (t *typeFixedString) Binary() []byte {
	return append(BinaryTypeFixedString[:], varintEncode(uint64(t.length))...)
}

func (t *typeFixedString) Write(w Writer, value []byte) error {
	if len(value) != t.length {
		return errors.Errorf("invalid length %d, expected %d", len(value), t.length)
	}

	_, err := w.Write(value)
	return err
}

func (t *typeFixedString) Read(r Reader) ([]byte, error) {
	buf := make([]byte, t.length)
	_, err := io.ReadAtLeast(r, buf, t.length)
	if err != nil {
		return nil, err
	}

	return buf[:t.length], nil
}

func (t *typeFixedString) WriteAny(w Writer, v any) error {
	value, ok := v.([]byte)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeFixedString) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
