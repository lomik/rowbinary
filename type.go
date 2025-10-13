package rowbinary

type Type[T any] interface {
	PreType[T]
	ID() uint64
}

type PreType[T any] interface {
	BaseType[T]
	WriteAny(w Writer, v any) error
	ScanAny(r Reader, v any) error
}

type BaseType[T any] interface {
	String() string
	Binary() []byte // https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding
	Write(w Writer, v T) error
	Scan(r Reader, v *T) error
}

type Any interface {
	String() string
	Binary() []byte
	ScanAny(r Reader, v any) error
	WriteAny(w Writer, v any) error
	ID() uint64
}

func Eq(a, b Any) bool {
	return a.ID() == b.ID()
}
