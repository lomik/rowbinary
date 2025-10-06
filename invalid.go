package rowbinary

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
	return BinaryTypeNothing[:]
}
func (t typeInvalid[T]) Write(w Writer, value T) error {
	return NewInvalidTypeError(t.msg)
}

func (t typeInvalid[T]) Scan(r Reader, v *T) error {
	return NewInvalidTypeError(t.msg)
}
