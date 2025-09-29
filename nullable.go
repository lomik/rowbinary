package rowbinary

import (
	"errors"
	"fmt"
)

var _ Type[*uint32] = Nullable(UInt32)

type typeNullable[V any] struct {
	id        uint64
	valueType Type[V]
	tbin      []byte
	tstr      string
}

func Nullable[V any](valueType Type[V]) *typeNullable[V] {
	tbin := append(BinaryTypeNullable[:], valueType.Binary()...)
	return &typeNullable[V]{
		valueType: valueType,
		id:        BinaryTypeID(tbin),
		tbin:      tbin,
		tstr:      fmt.Sprintf("Nullable(%s)", valueType.String()),
	}
}

func (t *typeNullable[V]) String() string {
	return t.tstr
}

func (t *typeNullable[V]) Binary() []byte {
	return t.tbin
}

func (t *typeNullable[V]) ID() uint64 {
	return t.id
}

func (t *typeNullable[V]) Write(w Writer, value *V) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.Write(w, *value)
}

func (t *typeNullable[V]) Read(r Reader) (*V, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == 0x01 {
		return nil, nil
	}

	value, err := t.valueType.Read(r)
	return &value, err
}

func (t *typeNullable[V]) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeNullable[V]) WriteAny(w Writer, v any) error {
	value, ok := v.(*V)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
