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

func (t *typeUVarint) Write(w Writer, value uint64) error {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], value)
	_, err := w.Write(buf[:n])
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
