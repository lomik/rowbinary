package rowbinary

import (
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
	err := UVarint.Write(w, uint64(len(value)))
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

func (t typeArray[V]) Read(r Reader) ([]V, error) {
	n, err := UVarint.Read(r)
	if err != nil {
		return nil, err
	}

	ret := make([]V, 0, int(n))
	for i := uint64(0); i < n; i++ {
		s, err := t.valueType.Read(r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}

	return ret, nil
}

func (t typeArray[V]) Scan(r Reader, v *[]V) error {
	n, err := UVarint.Read(r)
	if err != nil {
		return err
	}

	*v = (*v)[:0]
	for i := uint64(0); i < n; i++ {
		s, err := t.valueType.Read(r)
		if err != nil {
			return err
		}
		*v = append(*v, s)
	}

	return nil
}
