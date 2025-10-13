package rowbinary

import (
	"fmt"
)

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

func (t *typeWrapper[T]) ID() uint64 {
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

func (t typeWrapperAny[T]) ScanAny(r Reader, v any) error {
	var value T
	err := t.Scan(r, &value)
	if err != nil {
		return err
	}
	if p, ok := v.(**T); ok {
		*p = &value
		return nil
	}
	if p, ok := v.(*T); ok {
		*p = value
		return nil
	}

	if p, ok := v.(*any); ok {
		*p = value
		return nil
	}

	return fmt.Errorf("unexpected type %T", v)
}

func (t typeWrapperAny[T]) WriteAny(w Writer, v any) error {
	value, ok := v.(T)
	if !ok {
		return TypeMismatchError{}
	}
	return t.Write(w, value)
}
