package rowbinary

import (
	"errors"
	"fmt"
)

var MapUInt32UInt32 Type[map[uint32]uint32] = Map(UInt32, UInt32)

type typeMap[K comparable, V any] struct {
	keyType   Type[K]
	valueType Type[V]
}

func Map[K comparable, V any](keyType Type[K], valueType Type[V]) *typeMap[K, V] {
	return &typeMap[K, V]{
		keyType:   keyType,
		valueType: valueType,
	}
}

func (t *typeMap[K, V]) String() string {
	return fmt.Sprintf("Map(%s, %s)", t.keyType.String(), t.valueType.String())
}

func (t *typeMap[K, V]) Binary() []byte {
	return append(append(BinaryTypeMap[:], t.keyType.Binary()...), t.valueType.Binary()...)
}

func (t *typeMap[K, V]) Write(w Writer, value map[K]V) error {
	err := UVarint.Write(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		err = t.keyType.Write(w, k)
		if err != nil {
			return err
		}

		err = t.valueType.Write(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *typeMap[K, V]) Read(r Reader) (map[K]V, error) {
	n, err := UVarint.Read(r)
	if err != nil {
		return nil, err
	}

	ret := make(map[K]V, int(n))
	for i := uint64(0); i < n; i++ {
		k, err := t.keyType.Read(r)
		if err != nil {
			return nil, err
		}

		v, err := t.valueType.Read(r)
		if err != nil {
			return nil, err
		}
		ret[k] = v
	}

	return ret, nil
}

func (t *typeMap[K, V]) WriteAny(w Writer, v any) error {
	value, ok := v.(map[K]V)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeMap[K, V]) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
