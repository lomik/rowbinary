package rowbinary

import (
	"fmt"
)

// Nullable creates a Type for encoding and decoding nullable values in RowBinary format.
//
// It constructs a type handler for values that can be nil, represented as a pointer to V.
// In RowBinary, nullable values are encoded with a leading byte: 0x01 indicates null,
// and 0x00 indicates a present value followed by the value's encoding.
//
// Parameters:
//   - valueType: The Type handler for the underlying value type V.
//
// Returns:
//   - Type[*V]: A type instance that can read/write nullable values in RowBinary format.
//
// Note: Use a pointer (*V) to represent nullable values. A nil pointer encodes as null,
// and a non-nil pointer encodes the dereferenced value.
func Nullable[V any](valueType Type[V]) Type[*V] {
	return MakeTypeWrapAny(typeNullable[V]{
		valueType: valueType,
	})
}

type typeNullable[V any] struct {
	valueType Type[V]
}

func (t typeNullable[V]) String() string {
	return fmt.Sprintf("Nullable(%s)", t.valueType.String())
}

func (t typeNullable[V]) Binary() []byte {
	return append(BinaryTypeNullable[:], t.valueType.Binary()...)
}

func (t typeNullable[V]) Write(w Writer, value *V) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.Write(w, *value)
}

func (t typeNullable[V]) Scan(r Reader, v **V) error {
	b, err := r.ReadByte()
	if err != nil {
		return err
	}

	if b == 0x01 {
		*v = nil
		return nil
	}

	x, err := t.valueType.Read(r)
	if err != nil {
		return err
	}
	*v = &x

	return nil
}
