package types

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

var String Type[string] = &typeString{}

type typeString struct {
}

func (t *typeString) String() string {
	return "String"
}

func (t *typeString) Write(w Writer, value string) error {
	err := UVarint.Write(w, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(value))
	return err
}

func (t *typeString) Read(r Reader) (string, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return "", err
	}

	buf := make([]byte, n)
	_, err = io.ReadAtLeast(r, buf, int(n))
	if err != nil {
		return "", err
	}

	return string(buf[:n]), nil
}

func (t *typeString) WriteAny(w Writer, v any) error {
	value, ok := v.(string)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeString) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
