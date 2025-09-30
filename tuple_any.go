package rowbinary

import (
	"errors"
	"fmt"
	"strings"
)

type typeTupleAny struct {
	id         uint64
	valueTypes []Any
	tbin       []byte
	tstr       string
}

func TupleAny(valueTypes ...Any) typeTupleAny {
	var types []string
	for _, vt := range valueTypes {
		types = append(types, vt.String())
	}
	tbin := append(BinaryTypeTuple[:], UVarintEncode(uint64(len(valueTypes)))...)
	for _, vt := range valueTypes {
		tbin = append(tbin, vt.Binary()...)
	}
	return typeTupleAny{
		valueTypes: valueTypes,
		tbin:       tbin,
		tstr:       fmt.Sprintf("Tuple(%s)", strings.Join(types, ", ")),
		id:         BinaryTypeID(tbin),
	}
}

func (t typeTupleAny) String() string {
	return t.tstr
}

func (t typeTupleAny) Binary() []byte {
	return t.tbin
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

func (t typeTupleAny) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t typeTupleAny) WriteAny(w Writer, v any) error {
	value, ok := v.([]any)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeTupleAny) ID() uint64 {
	return t.id
}
