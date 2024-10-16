package rowbinary

import (
	"io"

	"github.com/pkg/errors"
)

type Reader interface {
	io.Reader
	io.ByteScanner
}

var ErrNotImplemented = errors.New("not implemented")
