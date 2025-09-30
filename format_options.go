package rowbinary

type useBinaryHeaderType struct {
	value bool
}

var _ FormatOption = WithUseBinaryHeader(false)

type formatOptions struct {
	format          Format
	columns         []Column
	useBinaryHeader bool
}

type FormatOption interface {
	applyFormatOption(*formatOptions)
}

func WithUseBinaryHeader(value bool) useBinaryHeaderType {
	return useBinaryHeaderType{
		value: value,
	}
}

func (o useBinaryHeaderType) applyFormatOption(opts *formatOptions) {
	opts.useBinaryHeader = o.value
}

func (o useBinaryHeaderType) applySelectOptions(opts *selectOptions) {
	opts.useBinaryHeader = o.value
	opts.formatOptions = append(opts.formatOptions, o)
}
