package rowbinary

import (
	"encoding/binary"

	"github.com/pkg/errors"
)

var UVarint Type[uint64] = &typeUVarint{}

type typeUVarint struct {
}

func (t *typeUVarint) String() string {
	return "UVarint"
}

func (t *typeUVarint) Write(w Writer, x uint64) error {
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

func (t *typeUVarint) Read(r Reader) (uint64, error) {
	return binary.ReadUvarint(r)
}

func (t *typeUVarint) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeUVarint) WriteAny(w Writer, v any) error {
	value, ok := v.(uint64)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
