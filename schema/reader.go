package schema

import (
	"bytes"
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/pluto-metrics/rowbinary"
)

type Reader struct {
	wrap     rowbinary.Reader
	options  options
	remote   []column
	index    int
	firstErr error
}

func NewReader(wrap io.Reader, opts ...Option) *Reader {
	r := &Reader{
		wrap: rowbinary.NewReader(wrap),
	}

	for _, opt := range opts {
		opt(&r.options)
	}

	return r
}

func (r *Reader) Err() error {
	return r.firstErr
}

func (r *Reader) Next() bool {
	_, err := r.wrap.ReadByte()
	if err != nil && err != io.EOF {
		r.setErr(err)
	}
	if err != nil {
		return false
	}
	if err = r.wrap.UnreadByte(); err != nil {
		return false
	}
	return true
}

func (r *Reader) next() {
	r.index++
}

func (r *Reader) setErr(err error) error {
	if r.firstErr == nil {
		r.firstErr = err
	}
	return r.firstErr
}

func (r *Reader) ReadHeader() error {
	if r.firstErr != nil {
		return r.firstErr
	}

	if r.options.format == RowBinary {
		return nil
	}

	if r.options.format == RowBinaryWithNames || r.options.format == RowBinaryWithNamesAndTypes {
		n, err := rowbinary.UVarint.Read(r.wrap)
		if err != nil {
			return r.setErr(err)
		}

		r.remote = make([]column, 0, n)

		for i := 0; i < int(n); i++ {
			name, err := rowbinary.String.Read(r.wrap)
			if err != nil {
				return r.setErr(err)
			}
			r.remote = append(r.remote, column{Name: name})
		}

		if r.options.format == RowBinaryWithNamesAndTypes {
			for i := 0; i < int(n); i++ {
				if r.options.isBinary {
					tp, err := rowbinary.DecodeBinaryType(r.wrap)
					if err != nil {
						return r.setErr(err)
					}
					r.remote[i].Type = tp
				} else {
					// @TODO: implement non-binary type decoding
					return r.setErr(errors.New("not implemented"))
				}
			}
		}
	}

	// @TODO: compare local and remote

	return nil
}

func Read[V any](r *Reader, tp rowbinary.Type[V]) (V, error) {
	var value V
	if r.firstErr != nil {
		return value, r.firstErr
	}

	if r.remote != nil {
		if !bytes.Equal(tp.Binary(), r.remote[r.index].Type.Binary()) {
			return value, fmt.Errorf("type mismatch. expected %s, got %s", r.remote[r.index].Type.String(), tp.String())
		}
	}

	value, err := tp.Read(r.wrap)
	r.next()
	return value, r.setErr(err)
}
