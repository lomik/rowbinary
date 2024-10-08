package rowbinary

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

var UInt64 Type[uint64] = &typeUInt64{}

type typeUInt64 struct {
}

func (t *typeUInt64) String() string {
	return "UInt64"
}

func (t *typeUInt64) Write(w Writer, value uint64) error {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], value)
	_, err := w.Write(buf[:])
	return err
}

func (t *typeUInt64) Read(r Reader) (uint64, error) {
	var buf [8]byte
	_, err := io.ReadAtLeast(r, buf[:], len(buf))
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf[:]), nil
}

func (t *typeUInt64) WriteAny(w Writer, v any) error {
	value, ok := v.(uint64)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeUInt64) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
