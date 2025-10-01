package rowbinary

import (
	"fmt"
)

type typeNullableAny struct {
	valueType Any
}

func NullableAny(valueType Any) Type[*any] {
	return MakeTypeWrapAny(typeNullableAny{
		valueType: valueType,
	})
}

func (t typeNullableAny) String() string {
	return fmt.Sprintf("Nullable(%s)", t.valueType.String())
}

func (t typeNullableAny) Binary() []byte {
	return append(BinaryTypeNullable[:], t.valueType.Binary()...)
}

func (t typeNullableAny) Write(w Writer, value *any) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.WriteAny(w, *value)
}

func (t typeNullableAny) Read(r Reader) (*any, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == 0x01 {
		return nil, nil
	}

	value, err := t.valueType.ReadAny(r)

	return &value, err
}
