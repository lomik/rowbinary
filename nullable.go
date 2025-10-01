package rowbinary

import (
	"fmt"
)

func Nullable[V any](valueType Type[V]) Type[*V] {
	return MakeTypeWrapAny(typeNullable[V]{
		valueType: valueType,
	})
}

type typeNullable[V any] struct {
	valueType Type[V]
}

func (t typeNullable[V]) String() string {
	return fmt.Sprintf("Nullable(%s)", t.valueType.String())
}

func (t typeNullable[V]) Binary() []byte {
	return append(BinaryTypeNullable[:], t.valueType.Binary()...)
}

func (t typeNullable[V]) Write(w Writer, value *V) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.Write(w, *value)
}

func (t typeNullable[V]) Read(r Reader) (*V, error) {
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
