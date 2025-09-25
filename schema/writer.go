package schema

import (
	"bytes"
	"io"

	"github.com/pkg/errors"

	"github.com/pluto-metrics/rowbinary"
)

type Writer struct {
	wrap     rowbinary.Writer
	options  options
	index    int
	firstErr error
}

func NewWriter(wrap io.Writer, opts ...Option) *Writer {
	r := &Writer{
		wrap: rowbinary.NewWriter(wrap),
	}

	for _, opt := range opts {
		opt(&r.options)
	}

	return r
}

func (w *Writer) Err() error {
	return w.firstErr
}

func (w *Writer) next() {
	w.index = (w.index + 1) % (len(w.options.columns))
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
	if w.options.format == RowBinary {
		return nil
	}
	if w.options.format == RowBinaryWithNames || w.options.format == RowBinaryWithNamesAndTypes {
		if err := rowbinary.UVarint.Write(w.wrap, uint64(len(w.options.columns))); err != nil {
			return w.setErr(err)
		}
		for i := 0; i < len(w.options.columns); i++ {
			if err := rowbinary.String.Write(w.wrap, w.options.columns[i].Name); err != nil {
				return w.setErr(err)
			}
		}

		if w.options.format == RowBinaryWithNamesAndTypes {
			for i := 0; i < len(w.options.columns); i++ {
				if w.options.isBinary {
					if _, err := w.wrap.Write(w.options.columns[i].Type.Binary()); err != nil {
						return w.setErr(err)
					}
				} else {
					if err := rowbinary.String.Write(w.wrap, w.options.columns[i].Type.String()); err != nil {
						return w.setErr(err)
					}
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
	err := w.options.columns[w.index].Type.WriteAny(w.wrap, value)
	w.next()
	return w.setErr(err)
}

func (w *Writer) WriteValues(values ...any) error {
	for i := range values {
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

	if !bytes.Equal(tp.Binary(), w.options.columns[w.index].Type.Binary()) {
		return errors.New("type mismatch")
	}

	err := tp.Write(w.wrap, value)
	w.next()
	return w.setErr(err)
}
