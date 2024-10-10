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
}

var ErrNotImplemented = errors.New("not implemented")
