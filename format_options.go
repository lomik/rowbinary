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
	opts.formatOptions = append(opts.formatOptions, o)
	if o.value {
		opts.params["output_format_binary_encode_types_in_binary_format"] = "1"
	} else {
		opts.params["output_format_binary_encode_types_in_binary_format"] = "0"
	}
}

func (o useBinaryHeaderType) applyInsertOptions(opts *insertOptions) {
	opts.formatOptions = append(opts.formatOptions, o)
	if o.value {
		opts.params["input_format_binary_decode_types_in_binary_format"] = "1"
	} else {
		opts.params["input_format_binary_decode_types_in_binary_format"] = "0"
	}
}
