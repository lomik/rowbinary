package rowbinary

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

var _ Any = ArrayAny(UInt32)

type typeTupleAny struct {
	valueTypes []Any
}

func TupleAny(valueTypes ...Any) *typeTupleAny {
	return &typeTupleAny{
		valueTypes: valueTypes,
	}
}

func (t *typeTupleAny) String() string {
	var types []string
	for _, vt := range t.valueTypes {
		types = append(types, vt.String())
	}
	return fmt.Sprintf("Tuple(%s)", strings.Join(types, ", "))
}

func (t *typeTupleAny) Binary() []byte {
	b := append(typeBinaryTuple[:], varintEncode(uint64(len(t.valueTypes)))...)
	for _, vt := range t.valueTypes {
		b = append(b, vt.Binary()...)
	}
	return b
}

func (t *typeTupleAny) Write(w Writer, value []any) error {
	if len(value) != len(t.valueTypes) {
		return errors.New("invalid tuple length")
	}

	for i, v := range value {
		err := t.valueTypes[i].WriteAny(w, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *typeTupleAny) Read(r Reader) ([]any, error) {
	ret := make([]any, 0, len(t.valueTypes))
	for i := 0; i < len(t.valueTypes); i++ {
		s, err := t.valueTypes[i].ReadAny(r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}

	return ret, nil
}

func (t *typeTupleAny) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeTupleAny) WriteAny(w Writer, v any) error {
	value, ok := v.([]any)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
