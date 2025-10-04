package rowbinary

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

type FormatReader struct {
	wrap     Reader
	options  formatOptions
	columns  []Column // from options of from remote
	index    int
	firstErr error
	doneInit bool // read header from remote on first Read or Next
}

func NewFormatReader(wrap io.Reader, opts ...FormatOption) *FormatReader {
	r := &FormatReader{
		wrap: NewReader(wrap),
		options: formatOptions{
			format:          RowBinary,
			useBinaryHeader: false,
		},
	}

	for _, opt := range opts {
		opt.applyFormatOption(&r.options)
	}

	return r
}

func (r *FormatReader) Err() error {
	return r.firstErr
}

func (r *FormatReader) setErr(err error) error {
	if r.firstErr == nil {
		r.firstErr = err
	}
	return r.firstErr
}

func (r *FormatReader) Next() bool {
	if err := r.check(); err != nil {
		return false
	}

	if len(r.columns) == 0 {
		return false
	}

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

func (r *FormatReader) nextColumn() {
	r.index = (r.index + 1) % len(r.columns)
}

func (r *FormatReader) check() error {
	if r.firstErr != nil {
		return r.firstErr
	}

	if r.doneInit {
		return nil
	}

	err := r.readHeader()
	if err != nil {
		return r.setErr(err)
	}

	r.doneInit = true
	return nil
}

// RowBinary has no header, columns must be set in options
func (r *FormatReader) readHeaderRowBinary() error {
	if r.options.columns == nil {
		return r.setErr(errors.New("columns must be set for RowBinary format"))
	}
	r.columns = r.options.columns
	return nil
}

// RowBinaryWithNames has header with column names, but no types
// types must be set in options
func (r *FormatReader) readHeaderRowBinaryWithNames() error {
	columnTypeMap := make(map[string]Any)
	for _, col := range r.options.columns {
		columnTypeMap[col.name] = col.tp
	}

	// read number of columns
	n, err := binary.ReadUvarint(r.wrap)
	if err != nil {
		return r.setErr(err)
	}

	remote := make([]Column, 0, n)

	// read names and match types from options
	for i := 0; i < int(n); i++ {
		var name string
		err = String.Scan(r.wrap, &name)
		if err != nil {
			return r.setErr(err)
		}
		tp, ok := columnTypeMap[name]
		if !ok {
			return r.setErr(fmt.Errorf("type for column %s is not defined", name))
		}
		remote = append(remote, Column{name: name, tp: tp})
	}

	r.columns = remote
	return nil
}

// RowBinaryWithNamesAndTypes has header with column names and types
// If types are set in options, they will be matched against remote types
func (r *FormatReader) readHeaderRowBinaryWithNamesAndTypes() error {
	columnTypeMap := make(map[string]Any)
	for _, col := range r.options.columns {
		columnTypeMap[col.name] = col.tp
	}

	// read number of columns
	n, err := binary.ReadUvarint(r.wrap)
	if err != nil {
		return r.setErr(err)
	}

	remote := make([]Column, 0, n)

	// read names
	for i := 0; i < int(n); i++ {
		var name string
		err = String.Scan(r.wrap, &name)
		if err != nil {
			return r.setErr(err)
		}
		remote = append(remote, Column{name: name})
	}

	for i := 0; i < int(n); i++ {
		if r.options.useBinaryHeader {
			tp, err := DecodeBinaryType(r.wrap)
			if err != nil {
				return r.setErr(err)
			}
			remote[i].tp = tp

		} else {
			var tpStr string
			err = String.Scan(r.wrap, &tpStr)
			if err != nil {
				return r.setErr(err)
			}
			tp, err := DecodeStringType(tpStr)
			if err != nil {
				return r.setErr(err)
			}
			remote[i].tp = tp
		}
	}

	// rewrite from options
	for i := 0; i < int(n); i++ {
		if tp, ok := columnTypeMap[remote[i].name]; ok {
			if !Eq(tp, remote[i].tp) {
				return r.setErr(fmt.Errorf("mismatched column type for column %s. expected %s, got %s", remote[i].name, tp.String(), remote[i].tp.String()))
			}
			remote[i].tp = tp
		}
	}

	r.columns = remote
	return nil
}

func (r *FormatReader) readHeader() error {
	if r.options.format == RowBinary {
		return r.readHeaderRowBinary()
	}
	if r.options.format == RowBinaryWithNames {
		return r.readHeaderRowBinaryWithNames()
	}
	if r.options.format == RowBinaryWithNamesAndTypes {
		return r.readHeaderRowBinaryWithNamesAndTypes()
	}

	return fmt.Errorf("unknown format: %v", r.options.format)
}

func (r *FormatReader) Columns() (*Columns, error) {
	if err := r.check(); err != nil {
		return nil, err
	}

	return &Columns{cols: r.columns}, nil
}

func (r *FormatReader) Column(i int) (Column, error) {
	if err := r.check(); err != nil {
		return Column{}, err
	}
	if i < 0 || i >= len(r.columns) {
		return Column{}, r.setErr(fmt.Errorf("column index out of bounds: %d", i))
	}
	return r.columns[i], nil
}

func (r *FormatReader) Scan(dest ...any) error {
	err := r.check()
	if err != nil {
		return err
	}
	for i := 0; i < len(dest); i++ {
		err = r.columns[r.index].tp.ScanAny(r.wrap, dest[i])
		if err != nil {
			return r.setErr(err)
		}
		r.nextColumn()
	}
	return nil
}

func Read[V any](r *FormatReader, tp Type[V]) (V, error) {
	var value V
	if err := r.check(); err != nil {
		return value, err
	}

	if tp.id() != r.columns[r.index].tp.id() {
		return value, r.setErr(fmt.Errorf("type mismatch. expected %s, got %s", r.columns[r.index].tp.String(), tp.String()))
	}

	err := tp.Scan(r.wrap, &value)
	r.nextColumn()
	return value, r.setErr(err)
}

func Scan[V any](r *FormatReader, tp Type[V], v *V) error {
	if err := r.check(); err != nil {
		return err
	}

	if tp.id() != r.columns[r.index].tp.id() {
		return r.setErr(fmt.Errorf("type mismatch. expected %s, got %s", r.columns[r.index].tp.String(), tp.String()))
	}

	err := tp.Scan(r.wrap, v)
	r.nextColumn()
	return r.setErr(err)
}
