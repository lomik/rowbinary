package rowbinary

import "errors"

func MakeType[T any](tp PreType[T]) Type[T] {
	return &typeWrapper[T]{
		PreType: tp,
		tbin:    tp.Binary(),
		tstr:    tp.String(),
		tid:     binaryTypeID(tp.Binary()),
	}
}

func WrapAny[T any](tp BaseType[T]) PreType[T] {
	return typeWrapperAny[T]{
		BaseType: tp,
	}
}

func MakeTypeWrapAny[T any](tp BaseType[T]) Type[T] {
	return MakeType(WrapAny(tp))
}

type typeWrapper[T any] struct {
	PreType[T]
	tid  uint64
	tbin []byte
	tstr string
}

func (t *typeWrapper[T]) id() uint64 {
	return t.tid
}

func (t typeWrapper[T]) String() string {
	return t.tstr
}

func (t *typeWrapper[T]) Binary() []byte {
	return t.tbin
}

type typeWrapperAny[T any] struct {
	BaseType[T]
}

func (t typeWrapperAny[T]) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t typeWrapperAny[T]) WriteAny(w Writer, v any) error {
	value, ok := v.(T)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
