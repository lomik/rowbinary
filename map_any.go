package rowbinary

import (
	"errors"
	"fmt"
	"slices"
)

var _ Any = MapAny(UInt32, UInt32)

type typeMapAny struct {
	id        uint64
	keyType   Any
	valueType Any
	tbin      []byte
	tstr      string
}

func MapAny(keyType Any, valueType Any) *typeMapAny {
	tbin := slices.Concat(BinaryTypeMap[:], keyType.Binary(), valueType.Binary())
	return &typeMapAny{
		keyType:   keyType,
		valueType: valueType,
		tbin:      tbin,
		tstr:      fmt.Sprintf("Map(%s, %s)", keyType.String(), valueType.String()),
		id:        BinaryTypeID(tbin),
	}
}

func (t *typeMapAny) String() string {
	return t.tstr
}

func (t *typeMapAny) Binary() []byte {
	return t.tbin
}

func (t *typeMapAny) ID() uint64 {
	return t.id
}

func (t *typeMapAny) Write(w Writer, value map[any]any) error {
	err := UVarint.Write(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		err = t.keyType.WriteAny(w, k)
		if err != nil {
			return err
		}

		err = t.valueType.WriteAny(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *typeMapAny) Read(r Reader) (map[any]any, error) {
	n, err := UVarint.Read(r)
	if err != nil {
		return nil, err
	}

	ret := make(map[any]any, int(n))
	for i := uint64(0); i < n; i++ {
		k, err := t.keyType.ReadAny(r)
		if err != nil {
			return nil, err
		}

		v, err := t.valueType.ReadAny(r)
		if err != nil {
			return nil, err
		}
		ret[k] = v
	}

	return ret, nil
}

func (t *typeMapAny) WriteAny(w Writer, v any) error {
	value, ok := v.(map[any]any)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeMapAny) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
