package types

type Type[T any] interface {
	String() string
	Read(r Reader) (T, error)
	Write(w Writer, v T) error
	ReadAny(r Reader) (any, error)
	WriteAny(w Writer, v any) error
}

type Any interface {
	String() string
	ReadAny(r Reader) (any, error)
	WriteAny(w Writer, v any) error
}
