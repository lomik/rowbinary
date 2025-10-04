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

// TupleNamedAny creates a Type for encoding and decoding named tuples with dynamic types in RowBinary format.
//
// It constructs a type handler for tuples where each element has a name and type,
// represented as a slice of 'any'. The tuple is encoded as a sequence of values in the order
// of the provided columns. The number of elements in the slice must match the number of columns.
//
// Parameters:
//   - columns: A variadic list of Column definitions, each containing a name and Any type handler.
//
// Returns:
//   - Type[[]any]: A type instance that can read/write named tuples with dynamic types in RowBinary format.
//
// Note: The length of the slice must exactly match the number of columns. Column names are used
// for metadata but do not affect the binary encoding. Type safety is not enforced at compile time.
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
