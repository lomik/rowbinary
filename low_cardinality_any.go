package rowbinary

import (
	"fmt"
)

var _ Any = NullableAny(UInt32)

type typeLowCardinalityAny struct {
	valueType Any
}

func LowCardinalityAny(valueType Any) *typeLowCardinalityAny {
	return &typeLowCardinalityAny{
		valueType: valueType,
	}
}

func (t *typeLowCardinalityAny) String() string {
	return fmt.Sprintf("LowCardinality(%s)", t.valueType.String())
}

func (t *typeLowCardinalityAny) Binary() []byte {
	return append(typeBinaryLowCardinality[:], t.valueType.Binary()...)
}

func (t *typeLowCardinalityAny) Write(w Writer, value any) error {
	return t.valueType.WriteAny(w, value)
}

func (t *typeLowCardinalityAny) Read(r Reader) (any, error) {
	return t.valueType.ReadAny(r)
}

func (t *typeLowCardinalityAny) ReadAny(r Reader) (any, error) {
	return t.valueType.ReadAny(r)
}

func (t *typeLowCardinalityAny) WriteAny(w Writer, v any) error {
	return t.valueType.WriteAny(w, v)
}
