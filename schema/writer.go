package schema

import (
	"github.com/pkg/errors"

	"github.com/pluto-metrics/rowbinary/types"
)

type Writer struct {
	wrap     types.Writer
	columns  []Column
	index    int
	firstErr error
}

func NewWriter(wrap types.Writer, columns ...Column) *Writer {
	return &Writer{
		wrap:    wrap,
		columns: columns,
	}
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

func (w *Writer) WriteWithNamesHeader() error {
	panic("not implemented")
	return nil
}

func (w *Writer) WriteWithNamesAndTypesHeader() error {
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

func (w *Writer) Values(values ...any) error {
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
