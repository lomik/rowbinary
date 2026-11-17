package rowbinary

import (
	"fmt"
	"slices"
)

func MakeType[T any](tp PreType[T]) Type[T] {
	// Own the encoding: simple types return slices of the package-level
	// BinaryType* arrays. Binary() hands out copies of it (see below).
	tbin := slices.Clone(tp.Binary())
	return &typeWrapper[T]{
		PreType: tp,
		tbin:    tbin,
		tstr:    tp.String(),
		tid:     binaryTypeID(tbin),
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

// Binary returns a copy of the type's encoding. Types are shared values
// (Int8, String, ...), so handing out the cached slice would let one caller
// corrupt the type for everyone, out of sync with its ID.
func (t *typeWrapper[T]) Binary() []byte {
	return slices.Clone(t.tbin)
}

func (t *typeWrapper[T]) binaryShared() []byte {
	return t.tbin
}

// sharedBinary is implemented by types made with MakeType.
type sharedBinary interface {
	binaryShared() []byte
}

// typeBinary returns tp's encoding without copying when possible. The result
// must not be modified; use it on per-value paths such as Dynamic.Write.
func typeBinary(tp Any) []byte {
	if s, ok := tp.(sharedBinary); ok {
		return s.binaryShared()
	}
	return tp.Binary()
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

	return TypeMismatchError{ExpectedType: t.String(), ActualType: fmt.Sprintf("%T", v)}
}

func (t typeWrapperAny[T]) WriteAny(w Writer, v any) error {
	value, ok := v.(T)
	if !ok && v == nil {
		// A nil interface has no dynamic type, so v.(any) fails too.
		// Permit it only when T itself is any, not for concrete value types.
		_, ok = any(&value).(*any)
	}
	if !ok {
		return TypeMismatchError{ExpectedType: t.String(), ActualType: fmt.Sprintf("%T", v)}
	}
	return t.Write(w, value)
}
