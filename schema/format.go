package schema

type Format int

const (
	RowBinary Format = iota
	RowBinaryWithNames
	RowBinaryWithNamesAndTypes
)
