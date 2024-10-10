package schema

import (
	"github.com/pkg/errors"

	"github.com/pluto-metrics/rowbinary"
)

type Writer struct {
	wrap     rowbinary.Writer
	columns  []column
	index    int
	firstErr error
	format   Format
}

func NewWriter(wrap rowbinary.Writer) *Writer {
	return &Writer{
		wrap:    wrap,
		columns: make([]column, 0),
	}
}

func (w *Writer) Column(name string, tp rowbinary.Any) *Writer {
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
	if w.firstErr != nil {
		return w.firstErr
	}
	if w.format == RowBinary {
		return nil
	}
	if w.format == RowBinaryWithNames || w.format == RowBinaryWithNamesAndTypes {
		if err := rowbinary.UVarint.Write(w.wrap, uint64(len(w.columns))); err != nil {
			return w.setErr(err)
		}
		for i := 0; i < len(w.columns); i++ {
			if err := rowbinary.String.Write(w.wrap, w.columns[i].Name); err != nil {
				return w.setErr(err)
			}
		}

		if w.format == RowBinaryWithNamesAndTypes {
			for i := 0; i < len(w.columns); i++ {
				if err := rowbinary.String.Write(w.wrap, w.columns[i].Type.String()); err != nil {
					return w.setErr(err)
				}
			}
		}
	}
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

func Write[V any](w *Writer, tp rowbinary.Type[V], value V) error {
	if w.firstErr != nil {
		return w.firstErr
	}
	// todo: optimize type check?
	if tp.String() != w.columns[w.index].Type.String() {
		return errors.New("type mismatch")
	}

	err := tp.Write(w.wrap, value)
	w.next()
	return w.setErr(err)
}
