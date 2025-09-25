package rowbinary

import (
	"fmt"
)

var _ Type[*uint32] = Nullable(UInt32)

type typeLowCardinality[V any] struct {
	valueType Type[V]
}

func LowCardinality[V any](valueType Type[V]) *typeLowCardinality[V] {
	return &typeLowCardinality[V]{
		valueType: valueType,
	}
}

func (t *typeLowCardinality[V]) String() string {
	return fmt.Sprintf("LowCardinality(%s)", t.valueType.String())
}

func (t *typeLowCardinality[V]) Binary() []byte {
	return append(BinaryTypeLowCardinality[:], t.valueType.Binary()...)
}

func (t *typeLowCardinality[V]) Write(w Writer, value V) error {
	return t.valueType.Write(w, value)
}

func (t *typeLowCardinality[V]) Read(r Reader) (V, error) {
	return t.valueType.Read(r)
}

func (t *typeLowCardinality[V]) ReadAny(r Reader) (any, error) {
	return t.valueType.ReadAny(r)
}

func (t *typeLowCardinality[V]) WriteAny(w Writer, v any) error {
	return t.valueType.WriteAny(w, v)
}
