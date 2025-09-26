package rowbinary

import "bytes"

type Type[T any] interface {
	String() string
	Binary() []byte // https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
	Read(r Reader) (T, error)
	Write(w Writer, v T) error
	ReadAny(r Reader) (any, error)
	WriteAny(w Writer, v any) error
}

type Any interface {
	String() string
	Binary() []byte
	ReadAny(r Reader) (any, error)
	WriteAny(w Writer, v any) error
}

func Eq(a, b Any) bool {
	return bytes.Equal(a.Binary(), b.Binary())
}
