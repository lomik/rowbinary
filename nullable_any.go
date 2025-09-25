package rowbinary

import (
	"fmt"

	"github.com/pkg/errors"
)

var _ Any = NullableAny(UInt32)

type typeNullableAny struct {
	valueType Any
}

func NullableAny(valueType Any) *typeNullableAny {
	return &typeNullableAny{
		valueType: valueType,
	}
}

func (t *typeNullableAny) String() string {
	return fmt.Sprintf("Nullable(%s)", t.valueType.String())
}

func (t *typeNullableAny) Binary() []byte {
	return append(typeBinaryNullable[:], t.valueType.Binary()...)
}

func (t *typeNullableAny) Write(w Writer, value *any) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.WriteAny(w, *value)
}

func (t *typeNullableAny) Read(r Reader) (any, error) {
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

func (t *typeNullableAny) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeNullableAny) WriteAny(w Writer, v any) error {
	if v == nil {
		return t.Write(w, nil)
	}
	value, ok := v.(*any)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
