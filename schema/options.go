package schema

import "github.com/pluto-metrics/rowbinary"

type column struct {
	Name string
	Type rowbinary.Any
}

type options struct {
	columns  []column
	isBinary bool
	format   format
}

type Option func(*options)

func Format(f format) Option {
	return func(o *options) {
		o.format = f
	}
}

// output_format_binary_encode_types_in_binary_format=true
// input_format_binary_decode_types_in_binary_format=true
// https://clickhouse.com/docs/interfaces/formats/RowBinary
func Binary(isBinary bool) Option {
	return func(o *options) {
		o.isBinary = isBinary
	}
}

func Column(name string, tp rowbinary.Any) Option {
	return func(o *options) {
		o.columns = append(o.columns, column{
			Name: name,
			Type: tp,
		})
	}
}
