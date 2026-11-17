package rowbinary

import "slices"

// binaryTypeInvalidMarker starts the encoding of an Invalid type. ClickHouse
// assigns no binary type code to 0xFF, so an Invalid type never shares an
// encoding (and therefore an ID) with Nothing or any real type, and a header
// written from it is rejected rather than read as a valid type.
const binaryTypeInvalidMarker = 0xFF

// Invalid returns a placeholder type for a constructor that cannot build a
// real one (for example Decimal with precision above 76). Every Write and Scan
// returns an error carrying msg.
func Invalid[T any](msg string) Type[T] {
	return MakeTypeWrapAny[T](typeInvalid[T]{msg: msg})
}

type typeInvalid[T any] struct {
	msg string
}

func (t typeInvalid[T]) String() string {
	return "Invalid"
}

func (t typeInvalid[T]) Binary() []byte {
	return slices.Concat([]byte{binaryTypeInvalidMarker}, StringEncode(t.msg))
}

func (t typeInvalid[T]) Write(w Writer, value T) error {
	return NewInvalidTypeError(t.msg)
}

func (t typeInvalid[T]) Scan(r Reader, v *T) error {
	return NewInvalidTypeError(t.msg)
}
