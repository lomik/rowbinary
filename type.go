package rowbinary

type Type[T any] interface {
	PreType[T]
	id() uint64
}

type PreType[T any] interface {
	BaseType[T]
	ReadAny(r Reader) (any, error)
	WriteAny(w Writer, v any) error
	ScanAny(r Reader, v any) error
}

type BaseType[T any] interface {
	String() string
	Binary() []byte // https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
	Read(r Reader) (T, error)
	Write(w Writer, v T) error
	Scan(r Reader, v *T) error
}

type Any interface {
	String() string
	Binary() []byte
	ReadAny(r Reader) (any, error)
	ScanAny(r Reader, v any) error
	WriteAny(w Writer, v any) error
	id() uint64
}

func Eq(a, b Any) bool {
	return a.id() == b.id()
}
