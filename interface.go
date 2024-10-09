package rowbinary

import (
	"io"

	"github.com/pkg/errors"
)

type Reader interface {
	io.Reader
	io.ByteReader
	Peek(n int) ([]byte, error)
}

type Writer interface {
	io.Writer
}

var ErrNotImplemented = errors.New("not implemented")
