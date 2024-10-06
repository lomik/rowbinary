package schema

import (
	"github.com/pkg/errors"
	"github.com/pluto-metrics/rowbinary/types"
)

type Reader struct {
	wrap        types.Reader
	columnTypes []types.Any
	columns     []Column
	index       int
	firstErr    error
}

func NewReader(wrap types.Reader, columnTypes ...types.Any) *Reader {
	return &Reader{
		wrap:        wrap,
		columnTypes: columnTypes,
	}
}

func (r *Reader) Err() error {
	return r.firstErr
}

func (r *Reader) next() {
	r.index = (r.index + 1) % (len(r.columns))
}

func (r *Reader) setErr(err error) error {
	if r.firstErr == nil {
		r.firstErr = err
	}
	return r.firstErr
}

func Read[V any](r *Reader, tp types.Type[V]) (V, error) {
	var value V

	// todo: optimize type check?
	if tp.String() != r.columnTypes[r.index].String() {
		return value, errors.New("type mismatch")
	}

	value, err := tp.Read(r.wrap)
	r.next()
	return value, r.setErr(err)
}
