package rowbinary

import (
	"fmt"
)

type typeNullableAny struct {
	valueType Any
}

// NullableAny creates a Type for encoding and decoding nullable values with dynamic types in RowBinary format.
//
// It constructs a type handler for values that can be nil, represented as a pointer to 'any'.
// This allows for nullable values where the type is determined at runtime. In RowBinary,
// nullable values are encoded with a leading byte: 0x01 indicates null, and 0x00 indicates
// a present value followed by the value's encoding using the provided Any type handler.
//
// Parameters:
//   - valueType: The Any type handler for the underlying value type.
//
// Returns:
//   - Type[*any]: A type instance that can read/write nullable values with dynamic types in RowBinary format.
//
// Note: Use a pointer (*any) to represent nullable values. A nil pointer encodes as null,
// and a non-nil pointer encodes the dereferenced value. Type safety is not enforced at compile time.
func NullableAny(valueType Any) Type[*any] {
	return MakeTypeWrapAny(typeNullableAny{
		valueType: valueType,
	})
}

func (t typeNullableAny) String() string {
	return fmt.Sprintf("Nullable(%s)", t.valueType.String())
}

func (t typeNullableAny) Binary() []byte {
	return append(BinaryTypeNullable[:], t.valueType.Binary()...)
}

func (t typeNullableAny) Write(w Writer, value *any) error {
	if value == nil {
		return w.WriteByte(0x01)
	}
	err := w.WriteByte(0x0)
	if err != nil {
		return err
	}
	return t.valueType.WriteAny(w, *value)
}

func (t typeNullableAny) Read(r Reader) (*any, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	if b == 0x01 {
		return nil, nil
	}

	value, err := t.valueType.ReadAny(r)

	return &value, err
}

func (t typeNullableAny) Scan(r Reader, v **any) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
