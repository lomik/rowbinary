package schema

type format int

const (
	RowBinary                  format = 0
	RowBinaryWithNames         format = 1
	RowBinaryWithNamesAndTypes format = 2
)
