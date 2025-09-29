package rowbinary

import (
	"fmt"
)

var _ Type[uint32] = LowCardinality(UInt32)

type typeLowCardinality[V any] struct {
	id        uint64
	valueType Type[V]
	tbin      []byte
	tstr      string
}

func LowCardinality[V any](valueType Type[V]) *typeLowCardinality[V] {
	tbin := append(BinaryTypeLowCardinality[:], valueType.Binary()...)
	return &typeLowCardinality[V]{
		valueType: valueType,
		tbin:      tbin,
		tstr:      fmt.Sprintf("LowCardinality(%s)", valueType.String()),
		id:        BinaryTypeID(tbin),
	}
}

func (t *typeLowCardinality[V]) String() string {
	return t.tstr
}

func (t *typeLowCardinality[V]) Binary() []byte {
	return t.tbin
}

func (t *typeLowCardinality[V]) ID() uint64 {
	return t.id
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
