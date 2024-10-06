package schema

import (
	"github.com/pkg/errors"

	"github.com/pluto-metrics/rowbinary/types"
)

type Writer struct {
	wrap     types.Writer
	columns  []column
	index    int
	firstErr error
	format   Format
}

func NewWriter(wrap types.Writer) *Writer {
	return &Writer{
		wrap:    wrap,
		columns: make([]column, 0),
	}
}

func (w *Writer) Column(name string, tp types.Any) *Writer {
	w.columns = append(w.columns, column{
		Name: name,
		Type: tp,
	})
	return w
}

func (w *Writer) Format(f Format) *Writer {
	w.format = f
	return w
}

func (w *Writer) Err() error {
	return w.firstErr
}

func (w *Writer) next() {
	w.index = (w.index + 1) % (len(w.columns))
}

func (w *Writer) setErr(err error) error {
	if w.firstErr == nil {
		w.firstErr = err
	}
	return w.firstErr
}

func (w *Writer) WriteHeader() error {
	panic("not implemented")
	return nil
}

func (w *Writer) single(value any) error {
	if w.firstErr != nil {
		return w.firstErr
	}
	err := w.columns[w.index].Type.WriteAny(w.wrap, value)
	w.next()
	return w.setErr(err)
}

func (w *Writer) WriteValues(values ...any) error {
	for i := 0; i < len(values); i++ {
		err := w.single(values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func Write[V any](w *Writer, tp types.Type[V], value V) error {
	// todo: optimize type check?
	if tp.String() != w.columns[w.index].Type.String() {
		return errors.New("type mismatch")
	}

	err := tp.Write(w.wrap, value)
	w.next()
	return w.setErr(err)
}
