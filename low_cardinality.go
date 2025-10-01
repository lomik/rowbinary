package rowbinary

import (
	"fmt"
)

var _ Type[uint32] = LowCardinality(UInt32)

func LowCardinality[V any](valueType Type[V]) Type[V] {
	return MakeTypeWrapAny(typeLowCardinality[V]{
		valueType: valueType,
	})
}

type typeLowCardinality[V any] struct {
	valueType Type[V]
}

func (t typeLowCardinality[V]) String() string {
	return fmt.Sprintf("LowCardinality(%s)", t.valueType.String())
}

func (t typeLowCardinality[V]) Binary() []byte {
	return append(BinaryTypeLowCardinality[:], t.valueType.Binary()...)
}

func (t typeLowCardinality[V]) Write(w Writer, value V) error {
	return t.valueType.Write(w, value)
}

func (t typeLowCardinality[V]) Read(r Reader) (V, error) {
	return t.valueType.Read(r)
}
