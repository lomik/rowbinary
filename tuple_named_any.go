package rowbinary

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type typeTupleNamedAny struct {
	id      uint64
	columns []Column
	tbin    []byte
	tstr    string
}

func TupleNameAny(columns ...Column) *typeTupleNamedAny {
	var types []string
	for _, col := range columns {
		types = append(types, col.String())
	}
	tbin := append(BinaryTypeTupleNamed[:], UVarintEncode(uint64(len(columns)))...)
	for _, col := range columns {
		tbin = slices.Concat(tbin, StringEncode(col.Name()), col.Type().Binary())
	}
	return &typeTupleNamedAny{
		id:      BinaryTypeID(tbin),
		columns: columns,
		tbin:    tbin,
		tstr:    fmt.Sprintf("Tuple(%s)", strings.Join(types, ", ")),
	}
}

func (t *typeTupleNamedAny) String() string {
	return t.tstr
}

func (t *typeTupleNamedAny) Binary() []byte {
	return t.tbin
}

func (t *typeTupleNamedAny) Write(w Writer, value []any) error {
	if len(value) != len(t.columns) {
		return errors.New("invalid tuple length")
	}

	for i, v := range value {
		err := t.columns[i].Type().WriteAny(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *typeTupleNamedAny) Read(r Reader) ([]any, error) {
	ret := make([]any, 0, len(t.columns))
	for i := 0; i < len(t.columns); i++ {
		s, err := t.columns[i].Type().ReadAny(r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}

	return ret, nil
}

func (t *typeTupleNamedAny) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeTupleNamedAny) WriteAny(w Writer, v any) error {
	value, ok := v.([]any)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeTupleNamedAny) ID() uint64 {
	return t.id
}
