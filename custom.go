package rowbinary

type customType[T any] struct {
	Type[T]
	name string
}

var Point Type[[]any] = Custom("Point", TupleAny(Float64, Float64))

func Custom[T any](name string, base Type[T]) Type[T] {
	return MakeType(&customType[T]{
		Type: base,
		name: name,
	})
}

func (t *customType[T]) String() string {
	return t.name
}

func (t *customType[T]) Binary() []byte {
	return append(BinaryTypeCustom[:], StringEncode(t.name)...)
}
