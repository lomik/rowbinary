package rowbinary

import (
	"errors"
	"fmt"
	"slices"
)

func MapKV[K comparable, V any](keyType Type[K], valueType Type[V]) Type[*KV[K, V]] {
	return MakeTypeWrapAny(typeMapKV[K, V]{
		keyType:   keyType,
		valueType: valueType,
	})
}

type typeMapKV[K comparable, V any] struct {
	keyType   Type[K]
	valueType Type[V]
}

func (t typeMapKV[K, V]) String() string {
	return fmt.Sprintf("Map(%s, %s)", t.keyType.String(), t.valueType.String())
}

func (t typeMapKV[K, V]) Binary() []byte {
	return slices.Concat(BinaryTypeMap[:], t.keyType.Binary(), t.valueType.Binary())
}

func (t typeMapKV[K, V]) Write(w Writer, value *KV[K, V]) error {
	err := UVarint.Write(w, uint64(value.Len()))
	if err != nil {
		return err
	}

	return value.Each(func(k K, v V) error {
		eachErr := t.keyType.Write(w, k)
		if eachErr != nil {
			return eachErr
		}

		eachErr = t.valueType.Write(w, v)
		if eachErr != nil {
			return eachErr
		}
		return nil
	})
}

func (t typeMapKV[K, V]) Read(r Reader) (*KV[K, V], error) {
	ret := NewKV[K, V]()
	n, err := UVarint.Read(r)
	if err != nil {
		return ret, err
	}

	for i := uint64(0); i < n; i++ {
		k, err := t.keyType.Read(r)
		if err != nil {
			return ret, err
		}

		v, err := t.valueType.Read(r)
		if err != nil {
			return ret, err
		}
		ret.Append(k, v)
	}

	return ret, nil
}

func (t typeMapKV[K, V]) WriteAny(w Writer, v any) error {
	value, ok := v.(*KV[K, V])
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeMapKV[K, V]) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t typeMapKV[K, V]) Scan(r Reader, ret **KV[K, V]) (err error) {
	if *ret == nil {
		*ret = NewKV[K, V]()
	}
	(*ret).Reset()

	n, err := UVarint.Read(r)
	if err != nil {
		return err
	}

	for i := uint64(0); i < n; i++ {
		k, err := t.keyType.Read(r)
		if err != nil {
			return err
		}

		v, err := t.valueType.Read(r)
		if err != nil {
			return err
		}
		(*ret).Append(k, v)
	}

	return nil
}
