package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

func MapKV[K any, V any](keyType Type[K], valueType Type[V]) Type[*KV[K, V]] {
	return MakeTypeWrapAny(typeMapKV[K, V]{
		keyType:   keyType,
		valueType: valueType,
	})
}

type typeMapKV[K any, V any] struct {
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
	err := VarintWrite(w, uint64(value.Len()))
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

func (t typeMapKV[K, V]) Scan(r Reader, ret **KV[K, V]) error {
	if *ret == nil {
		*ret = NewKV[K, V]()
	}
	(*ret).Reset()

	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	for i := uint64(0); i < n; i++ {
		var k K
		var v V
		err = t.keyType.Scan(r, &k)
		if err != nil {
			return err
		}

		err = t.valueType.Scan(r, &v)
		if err != nil {
			return err
		}
		(*ret).Append(k, v)
	}

	return nil
}
