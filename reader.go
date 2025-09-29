package rowbinary

import (
	"bufio"
	"io"
)

type Reader interface {
	io.Reader
	io.ByteScanner
	Peek(n int) ([]byte, error)
	Discard(n int) (discarded int, err error)
}

func NewReader(r io.Reader) Reader {
	return bufio.NewReaderSize(r, 1024*1024)
}
