package rowbinary

import (
	"bufio"
	"io"
)

type Reader interface {
	io.Reader
	io.ByteScanner
	buffer() []byte // 16 bytes buffer for decoding
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
