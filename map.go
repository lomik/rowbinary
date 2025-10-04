package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// Map creates a Type for encoding and decoding maps in RowBinary format.
//
// It constructs a type handler for maps with keys of type K and values of type V,
// using the provided keyType and valueType for serialization. The map is encoded
// as a sequence of key-value pairs, preceded by the number of entries as a UVarint.
//
// Parameters:
//   - keyType: The Type handler for the map keys (K must be comparable).
//   - valueType: The Type handler for the map values.
//
// Returns:
//   - Type[map[K]V]: A type instance that can read/write maps in RowBinary format.
//
// Note: The order of key-value pairs in the encoded output is not guaranteed to match
// the iteration order of the map, as Go maps are unordered.
func Map[K comparable, V any](keyType Type[K], valueType Type[V]) Type[map[K]V] {
	return MakeTypeWrapAny(typeMap[K, V]{
		keyType:   keyType,
		valueType: valueType,
	})
}

type typeMap[K comparable, V any] struct {
	keyType   Type[K]
	valueType Type[V]
}

func (t typeMap[K, V]) String() string {
	return fmt.Sprintf("Map(%s, %s)", t.keyType.String(), t.valueType.String())
}

func (t typeMap[K, V]) Binary() []byte {
	return slices.Concat(BinaryTypeMap[:], t.keyType.Binary(), t.valueType.Binary())
}

func (t typeMap[K, V]) Write(w Writer, value map[K]V) error {
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

func (t typeMap[K, V]) Scan(r Reader, ret *map[K]V) (err error) {
	*ret = make(map[K]V)

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
		(*ret)[k] = v
	}

	return nil
}
