package rowbinary

import (
	"fmt"
	"slices"
)

// MapAny creates a Type for encoding and decoding maps with dynamic types in RowBinary format.
//
// It constructs a type handler for maps where keys and values are of type 'any',
// using the provided Any type handlers for serialization. This allows for heterogeneous
// key-value pairs where types are determined at runtime. The map is encoded as a sequence
// of key-value pairs, preceded by the number of entries as a UVarint.
//
// Parameters:
//   - keyType: The Any type handler for the map keys.
//   - valueType: The Any type handler for the map values.
//
// Returns:
//   - Type[map[any]any]: A type instance that can read/write maps with dynamic types in RowBinary format.
//
// Note: Since keys and values are 'any', type safety is not enforced at compile time.
// Ensure that the provided Any types match the actual data types to avoid runtime errors.
// The order of key-value pairs in the encoded output is not guaranteed.
func MapAny(keyType Any, valueType Any) Type[map[any]any] {
	return MakeTypeWrapAny(typeMapAny{
		keyType:   keyType,
		valueType: valueType,
	})
}

type typeMapAny struct {
	keyType   Any
	valueType Any
}

func (t typeMapAny) String() string {
	return fmt.Sprintf("Map(%s, %s)", t.keyType.String(), t.valueType.String())
}

func (t typeMapAny) Binary() []byte {
	return slices.Concat(BinaryTypeMap[:], t.keyType.Binary(), t.valueType.Binary())
}

func (t typeMapAny) Write(w Writer, value map[any]any) error {
	err := UVarint.Write(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for k, v := range value {
		err = t.keyType.WriteAny(w, k)
		if err != nil {
			return err
		}

		err = t.valueType.WriteAny(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t typeMapAny) Read(r Reader) (map[any]any, error) {
	n, err := UVarint.Read(r)
	if err != nil {
		return nil, err
	}

	ret := make(map[any]any, int(n))
	for i := uint64(0); i < n; i++ {
		k, err := t.keyType.ReadAny(r)
		if err != nil {
			return nil, err
		}

		v, err := t.valueType.ReadAny(r)
		if err != nil {
			return nil, err
		}
		ret[k] = v
	}

	return ret, nil
}

func (t typeMapAny) Scan(r Reader, v *map[any]any) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
