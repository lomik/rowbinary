package rowbinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unsafe"
)

func toBytes(s string) []byte {
	// unsafe.StringData is unspecified for the empty string, so we provide a strict interpretation
	if len(s) == 0 {
		return nil
	}
	// Copied from go 1.20.1 os.File.WriteString
	// https://github.com/golang/go/blob/202a1a57064127c3f19d96df57b9f9586145e21c/src/os/file.go#L246
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

var String Type[string] = typeString{}

type typeString struct{}

var typeStringID = BinaryTypeID(BinaryTypeString[:])

func (t typeString) String() string {
	return "String"
}

func (t typeString) Binary() []byte {
	return BinaryTypeString[:]
}

func (t typeString) ID() uint64 {
	return typeStringID
}

func (t typeString) Write(w Writer, value string) error {
	err := UVarint.Write(w, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = w.Write(toBytes(value))
	return err
}

func (t typeString) Read(r Reader) (string, error) {
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

func (t typeString) WriteAny(w Writer, v any) error {
	value, ok := v.(string)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeString) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func StringEncode(s string) []byte {
	var b bytes.Buffer
	w := NewWriter(&b)
	String.Write(w, s)
	return b.Bytes()
}
