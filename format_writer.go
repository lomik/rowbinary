package rowbinary

import (
	"fmt"
	"io"
)

type FormatWriter struct {
	wrap     Writer
	options  formatOptions
	index    int
	firstErr error
	doneInit bool
}

func NewFormatWriter(wrap io.Writer, opts ...FormatOption) *FormatWriter {
	w := &FormatWriter{
		wrap: NewWriter(wrap),
	}

	for _, opt := range opts {
		opt.applyFormatOption(&w.options)
	}

	return w
}

func (w *FormatWriter) Err() error {
	return w.firstErr
}

func (w *FormatWriter) nextColumn() {
	w.index = (w.index + 1) % (len(w.options.columns))
}

func (w *FormatWriter) setErr(err error) error {
	if w.firstErr == nil {
		w.firstErr = err
	}
	return w.firstErr
}

func (w *FormatWriter) check() error {
	if w.firstErr != nil {
		return w.firstErr
	}

	if w.doneInit {
		return nil
	}

	if len(w.options.columns) == 0 {
		return w.setErr(fmt.Errorf("no columns defined in options"))
	}

	err := w.writeHeader()
	if err != nil {
		return w.setErr(err)
	}

	w.doneInit = true
	return nil
}

func (w *FormatWriter) WriteHeader() error {
	return w.check()
}

func (w *FormatWriter) writeHeader() error {
	if w.firstErr != nil {
		return w.firstErr
	}
	if w.options.format == RowBinary {
		return nil
	}
	if w.options.format == RowBinaryWithNames || w.options.format == RowBinaryWithNamesAndTypes {
		if err := UVarint.Write(w.wrap, uint64(len(w.options.columns))); err != nil {
			return w.setErr(err)
		}
		for i := 0; i < len(w.options.columns); i++ {
			if err := String.Write(w.wrap, w.options.columns[i].name); err != nil {
				return w.setErr(err)
			}
		}

		if w.options.format == RowBinaryWithNamesAndTypes {
			for i := 0; i < len(w.options.columns); i++ {
				if w.options.useBinaryHeader {
					if _, err := w.wrap.Write(w.options.columns[i].tp.Binary()); err != nil {
						return w.setErr(err)
					}
				} else {
					if err := String.Write(w.wrap, w.options.columns[i].tp.String()); err != nil {
						return w.setErr(err)
					}
				}
			}
		}
	}
	return nil
}

func (w *FormatWriter) WriteAny(values ...any) error {
	if err := w.check(); err != nil {
		return err
	}

	for i := range values {
		if err := w.options.columns[w.index].tp.WriteAny(w.wrap, values[i]); err != nil {
			return w.setErr(err)
		}
		w.nextColumn()
	}
	return nil
}

func Write[V any](w *FormatWriter, tp Type[V], value V) error {
	if err := w.check(); err != nil {
		return err
	}

	if !Eq(tp, w.options.columns[w.index].tp) {
		return fmt.Errorf("type mismatch. expected %s, got %s", w.options.columns[w.index].tp.String(), tp.String())
	}

	err := tp.Write(w.wrap, value)
	w.nextColumn()
	return w.setErr(err)
}
