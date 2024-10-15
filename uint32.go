package rowbinary

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

var UInt32 Type[uint32] = &typeUInt32{}

type typeUInt32 struct {
}

func (t *typeUInt32) String() string {
	return "UInt32"
}

func (t *typeUInt32) Write(w Writer, v uint32) error {
	var err error
	if err = w.WriteByte(byte(v)); err != nil {
		return err
	}
	if err = w.WriteByte(byte(v >> 8)); err != nil {
		return err
	}
	if err = w.WriteByte(byte(v >> 16)); err != nil {
		return err
	}
	return w.WriteByte(byte(v >> 24))
}

func (t *typeUInt32) Read(r Reader) (uint32, error) {
	var buf [4]byte
	_, err := io.ReadAtLeast(r, buf[:], len(buf))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf[:]), nil
}

func (t *typeUInt32) WriteAny(w Writer, v any) error {
	value, ok := v.(uint32)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeUInt32) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
