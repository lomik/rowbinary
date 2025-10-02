package rowbinary

import (
	"errors"
	"fmt"
	"strings"
)

func TupleAny(valueTypes ...Any) Type[[]any] {
	return MakeTypeWrapAny(typeTupleAny{
		valueTypes: valueTypes,
	})
}

type typeTupleAny struct {
	valueTypes []Any
}

func (t typeTupleAny) String() string {
	var types []string
	for _, vt := range t.valueTypes {
		types = append(types, vt.String())
	}
	return fmt.Sprintf("Tuple(%s)", strings.Join(types, ", "))
}

func (t typeTupleAny) Binary() []byte {
	tbin := append(BinaryTypeTuple[:], UVarintEncode(uint64(len(t.valueTypes)))...)
	for _, vt := range t.valueTypes {
		tbin = append(tbin, vt.Binary()...)
	}
	return tbin
}

func (t typeTupleAny) Write(w Writer, value []any) error {
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

func (t typeTupleAny) Read(r Reader) ([]any, error) {
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

func (t typeTupleAny) Scan(r Reader, v *[]any) error {
	*v = (*v)[:0]
	for i := 0; i < len(t.valueTypes); i++ {
		s, err := t.valueTypes[i].ReadAny(r)
		if err != nil {
			return err
		}
		*v = append(*v, s)
	}

	return nil
}
