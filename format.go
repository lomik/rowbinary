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

func (f Format) applySelectOptions(o *selectOptions) {
	o.formatOptions = append(o.formatOptions, f)
	o.headers["X-ClickHouse-Format"] = f.String()
}

func (f Format) applyInsertOptions(o *insertOptions) {
	o.formatOptions = append(o.formatOptions, f)
	o.format = f
}

func (f Format) applyClientOptions(opts *clientOptions) {
	opts.defaultSelect = append(opts.defaultSelect, f)
	opts.defaultInsert = append(opts.defaultInsert, f)
}
