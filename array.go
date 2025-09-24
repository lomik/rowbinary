package rowbinary

import (
	"fmt"

	"github.com/pkg/errors"
)

var _ Type[[]uint32] = Array(UInt32)

type typeArray[V any] struct {
	valueType Type[V]
}

func Array[V any](valueType Type[V]) *typeArray[V] {
	return &typeArray[V]{
		valueType: valueType,
	}
}

func (t *typeArray[V]) String() string {
	return fmt.Sprintf("Array(%s)", t.valueType.String())
}

func (t *typeArray[V]) Binary() []byte {
	return append(typeBinaryArray[:], t.valueType.Binary()...)
}

func (t *typeArray[V]) Write(w Writer, value []V) error {
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

func (t *typeArray[V]) Read(r Reader) ([]V, error) {
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

func (t *typeArray[V]) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t *typeArray[V]) WriteAny(w Writer, v any) error {
	value, ok := v.([]V)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
