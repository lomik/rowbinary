package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

var _ Type[[]uint32] = Array(UInt32)

type typeArray[V any] struct {
	id        uint64
	valueType Type[V]
	tbin      []byte
	tstr      string
}

func Array[V any](valueType Type[V]) Type[[]V] {
	return MakeTypeWrapAny(typeArray[V]{valueType: valueType})
}

func (t typeArray[V]) String() string {
	return fmt.Sprintf("Array(%s)", t.valueType.String())
}

func (t typeArray[V]) Binary() []byte {
	return slices.Concat(BinaryTypeArray[:], t.valueType.Binary())
}

func (t typeArray[V]) Write(w Writer, value []V) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		err = t.valueType.Write(w, value[i])
		if err != nil {
			return err
		}
	}
	return err
}

func (t typeArray[V]) Scan(r Reader, v *[]V) error {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if *v == nil {
		*v = make([]V, int(n))
	} else if len(*v) >= int(n) {
		*v = (*v)[:n]
	} else {
		*v = append(*v, make([]V, int(n)-len(*v))...)
	}

	for i := 0; i < int(n); i++ {
		if err := t.valueType.Scan(r, &(*v)[i]); err != nil {
			return err
		}
	}

	return nil
}
