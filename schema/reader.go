package schema

import (
	"io"

	"github.com/pkg/errors"
	"github.com/pluto-metrics/rowbinary"
)

type Reader struct {
	wrap        rowbinary.Reader
	columnTypes []rowbinary.Any
	columns     []column
	index       int
	firstErr    error
	format      Format
}

func NewReader(wrap rowbinary.Reader) *Reader {
	return &Reader{
		wrap:        wrap,
		columnTypes: make([]rowbinary.Any, 0),
	}
}

func (r *Reader) Column(tp rowbinary.Any) *Reader {
	r.columnTypes = append(r.columnTypes, tp)
	return r
}

func (r *Reader) Format(f Format) *Reader {
	r.format = f
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
	r.index = (r.index + 1) % (len(r.columnTypes))
}

func (r *Reader) setErr(err error) error {
	if r.firstErr == nil {
		r.firstErr = err
	}
	return r.firstErr
}

func (r *Reader) ReadHeader() error {
	panic("not implemented")
	return nil
}

func Read[V any](r *Reader, tp rowbinary.Type[V]) (V, error) {
	var value V

	// todo: optimize type check?
	if tp.String() != r.columnTypes[r.index].String() {
		return value, errors.New("type mismatch")
	}

	value, err := tp.Read(r.wrap)
	r.next()
	return value, r.setErr(err)
}
