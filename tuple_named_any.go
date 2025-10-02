package rowbinary

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

type typeTupleNamedAny struct {
	columns []Column
}

func TupleNamedAny(columns ...Column) Type[[]any] {
	return MakeTypeWrapAny(typeTupleNamedAny{
		columns: columns,
	})
}

func (t typeTupleNamedAny) String() string {
	var types []string
	for _, col := range t.columns {
		types = append(types, col.String())
	}
	return fmt.Sprintf("Tuple(%s)", strings.Join(types, ", "))
}

func (t typeTupleNamedAny) Binary() []byte {
	tbin := append(BinaryTypeTupleNamed[:], UVarintEncode(uint64(len(t.columns)))...)
	for _, col := range t.columns {
		tbin = slices.Concat(tbin, StringEncode(col.Name()), col.Type().Binary())
	}
	return tbin
}

func (t typeTupleNamedAny) Write(w Writer, value []any) error {
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

func (t typeTupleNamedAny) Read(r Reader) ([]any, error) {
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

func (t typeTupleNamedAny) Scan(r Reader, v *[]any) error {
	*v = (*v)[:0]
	for i := 0; i < len(t.columns); i++ {
		s, err := t.columns[i].Type().ReadAny(r)
		if err != nil {
			return err
		}
		*v = append(*v, s)
	}

	return nil
}
