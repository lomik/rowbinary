package schema

import (
	"bytes"
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
	useBinary   bool
}

func NewReader(wrap io.Reader) *Reader {
	return &Reader{
		wrap:        rowbinary.NewReader(wrap),
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

// output_format_binary_encode_types_in_binary_format=true
// https://clickhouse.com/docs/interfaces/formats/RowBinary
func (r *Reader) UseBinary(use bool) *Reader {
	r.useBinary = use
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
	if r.firstErr != nil {
		return r.firstErr
	}

	if r.format == RowBinary {
		return nil
	}

	if r.format == RowBinaryWithNames || r.format == RowBinaryWithNamesAndTypes {
		n, err := rowbinary.UVarint.Read(r.wrap)
		if err != nil {
			return r.setErr(err)
		}

		if int(n) != len(r.columnTypes) {
			return r.setErr(errors.New("columns count mismatch"))
		}

		r.columns = make([]column, 0, len(r.columnTypes))

		for i := 0; i < len(r.columnTypes); i++ {
			name, err := rowbinary.String.Read(r.wrap)
			if err != nil {
				return r.setErr(err)
			}
			r.columns = append(r.columns, column{Name: name, Type: r.columnTypes[i]})
		}

		if r.format == RowBinaryWithNamesAndTypes {
			for i := 0; i < len(r.columnTypes); i++ {
				if r.useBinary {
					expectedType := r.columnTypes[i].Binary()
					tpHeader := make([]byte, len(expectedType))
					n, err := r.wrap.Read(tpHeader)
					if err != nil {
						return r.setErr(err)
					}
					if n != len(tpHeader) || !bytes.Equal(tpHeader, expectedType) {
						return r.setErr(errors.New("column type mismatch"))
					}
				} else {
					tp, err := rowbinary.String.Read(r.wrap)
					if err != nil {
						return r.setErr(err)
					}
					if r.columnTypes[i].String() != tp {
						return r.setErr(errors.New("column type mismatch"))
					}
				}
			}
		}
	}

	return nil
}

func Read[V any](r *Reader, tp rowbinary.Type[V]) (V, error) {
	var value V
	if r.firstErr != nil {
		return value, r.firstErr
	}

	// todo: optimize type check?
	if tp.String() != r.columnTypes[r.index].String() {
		return value, errors.New("type mismatch")
	}

	value, err := tp.Read(r.wrap)
	r.next()
	return value, r.setErr(err)
}
