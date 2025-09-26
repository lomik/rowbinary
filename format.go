package rowbinary

import "slices"

type Format int

var _ FormatOption = RowBinary

const (
	RowBinary                  Format = 0
	RowBinaryWithNames         Format = 1
	RowBinaryWithNamesAndTypes Format = 2
)

func (f Format) In(other ...Format) bool {
	return slices.Contains(other, f)
}

func (f Format) String() string {
	switch f {
	case RowBinary:
		return "RowBinary"
	case RowBinaryWithNames:
		return "RowBinaryWithNames"
	case RowBinaryWithNamesAndTypes:
		return "RowBinaryWithNamesAndTypes"
	default:
		return "Unknown"
	}
}

func (f Format) Eq(other Format) bool {
	return f == other
}

func (f Format) applyFormatOption(o *formatOptions) {
	o.format = f
}
