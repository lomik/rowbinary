package rowbinary

import (
	"errors"
	"fmt"
	"io"
)

var _ Type[[]byte] = FixedString(10)

func FixedString(length int) *typeFixedString {
	tbin := append(BinaryTypeFixedString[:], UVarintEncode(uint64(length))...)
	return &typeFixedString{
		length: length,
		tbin:   tbin,
		tstr:   fmt.Sprintf("FixedString(%d)", length),
		id:     BinaryTypeID(tbin),
	}
}

type typeFixedString struct {
	id     uint64
	length int
	tbin   []byte
	tstr   string
}

func (t *typeFixedString) String() string {
	return t.tstr
}

func (t *typeFixedString) Binary() []byte {
	return t.tbin
}

func (t *typeFixedString) ID() uint64 {
	return t.id
}

func (t *typeFixedString) Write(w Writer, value []byte) error {
	if len(value) != t.length {
		return fmt.Errorf("invalid length %d, expected %d", len(value), t.length)
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
