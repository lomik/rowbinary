package rowbinary

import (
	"io"

	"github.com/pkg/errors"
)

var UInt8 Type[uint8] = &typeUInt8{}

type typeUInt8 struct {
}

func (t *typeUInt8) String() string {
	return "UInt8"
}

func (t *typeUInt8) Write(w Writer, value uint8) error {
	var buf [1]byte
	buf[0] = byte(value)
	_, err := w.Write(buf[:])
	return err
}

func (t *typeUInt8) Read(r Reader) (uint8, error) {
	var buf [1]byte
	_, err := io.ReadAtLeast(r, buf[:], len(buf))
	if err != nil {
		return 0, err
	}
	return buf[0], nil
}

func (t *typeUInt8) WriteAny(w Writer, v any) error {
	value, ok := v.(uint8)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeUInt8) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
