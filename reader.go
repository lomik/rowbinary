package rowbinary

import (
	"bufio"
	"io"
)

type Reader interface {
	io.Reader
	io.ByteScanner
	buffer() []byte // 16 bytes buffer for decoding
	// ReadBytes reads exactly ln bytes from the underlying reader into internal buffer. And returns it
	// You can read up to 16 bytes
	// It is unsafe to use the returned slice after next call to ReadBytes
	ReadBytes(ln int) ([]byte, error)
}

type byteReader interface {
	io.Reader
	io.ByteScanner
}

type reader struct {
	byteReader
	buf [16]byte
}

func newByteReader(r io.Reader) byteReader {
	if br, ok := r.(byteReader); ok {
		return br
	}
	return bufio.NewReader(r)
}

func NewReader(r io.Reader) Reader {
	return &reader{
		byteReader: newByteReader(r),
	}
}

func (r *reader) buffer() []byte {
	return r.buf[:]
}

// ReadBytes reads exactly ln bytes from the underlying reader into internal buffer. And returns it
// You can read up to 16 bytes
// It is unsafe to use the returned slice after next call to ReadBytes
func (r *reader) ReadBytes(ln int) ([]byte, error) {
	n, err := r.Read(r.buf[:ln])
	if n == ln {
		return r.buf[:ln], nil
	}

	for n < ln && err == nil {
		var nn int
		nn, err = r.Read(r.buf[n:ln])
		n += nn
	}
	if n >= ln {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return r.buf[:ln], err
}
