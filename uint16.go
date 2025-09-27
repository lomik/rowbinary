package rowbinary

import (
	"encoding/binary"
	"errors"
	"io"
)

var UInt16 Type[uint16] = &typeUInt16{}

type typeUInt16 struct {
}

func (t *typeUInt16) String() string {
	return "UInt16"
}

func (t *typeUInt16) Binary() []byte {
	return BinaryTypeUInt16[:]
}

func (t *typeUInt16) Write(w Writer, v uint16) error {
	binary.LittleEndian.PutUint16(w.buffer(), v)
	_, err := w.Write(w.buffer()[:2])
	return err
}

func (t *typeUInt16) Read(r Reader) (uint16, error) {
	_, err := io.ReadAtLeast(r, r.buffer()[:2], 2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(r.buffer()[:2]), nil
}

func (t *typeUInt16) WriteAny(w Writer, v any) error {
	value, ok := v.(uint16)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeUInt16) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
