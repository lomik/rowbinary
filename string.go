package rowbinary

import (
	"bufio"
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

var String Type[string] = MakeTypeWrapAny[string](typeString{})

type typeString struct{}

func (t typeString) String() string {
	return "String"
}

func (t typeString) Binary() []byte {
	return BinaryTypeString[:]
}

func (t typeString) Write(w Writer, value string) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = w.Write(toBytes(value))
	return err
}

func (t typeString) Scan(r Reader, v *string) (err error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	// Fast path: the value fits in the reader's buffer, decode without allocating.
	buf, err := r.Peek(int(n))
	if err == nil {
		*v = string(buf[:n])
		_, err = r.Discard(int(n))
		return err
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		return err
	}

	// Slow path: the value is larger than the reader's buffer; Peek can never
	// return it, so read it into a fresh allocation via ReadFull.
	b := make([]byte, n)
	if _, err = io.ReadFull(r, b); err != nil {
		return err
	}
	*v = string(b)
	return nil
}
