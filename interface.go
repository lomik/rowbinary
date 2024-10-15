package rowbinary

import (
	"io"

	"github.com/pkg/errors"
)

type Reader interface {
	io.Reader
	io.ByteScanner
}

type Writer interface {
	io.Writer
	io.ByteWriter
	Buffer() []byte // returns 16 bytes buffer for encoding
}

var ErrNotImplemented = errors.New("not implemented")
