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

func (t *typeUInt8) Binary() []byte {
	return BinaryTypeUInt8[:]
}

func (t *typeUInt8) Write(w Writer, value uint8) error {
	return w.WriteByte(value)
}

func (t *typeUInt8) Read(r Reader) (uint8, error) {
	_, err := io.ReadAtLeast(r, r.buffer()[:1], 1)
	if err != nil {
		return 0, err
	}
	return r.buffer()[0], nil
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
