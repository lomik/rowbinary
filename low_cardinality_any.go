package rowbinary

import (
	"fmt"
)

var _ Any = LowCardinalityAny(UInt32)

type typeLowCardinalityAny struct {
	id        uint64
	valueType Any
	tbin      []byte
	tstr      string
}

func LowCardinalityAny(valueType Any) *typeLowCardinalityAny {
	tbin := append(BinaryTypeLowCardinality[:], valueType.Binary()...)
	return &typeLowCardinalityAny{
		valueType: valueType,
		tbin:      tbin,
		tstr:      fmt.Sprintf("LowCardinality(%s)", valueType.String()),
		id:        BinaryTypeID(tbin),
	}
}

func (t *typeLowCardinalityAny) String() string {
	return t.tstr
}

func (t *typeLowCardinalityAny) Binary() []byte {
	return t.tbin
}

func (t *typeLowCardinalityAny) ID() uint64 {
	return t.id
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
