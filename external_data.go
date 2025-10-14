package rowbinary

type externalData struct {
	name          string
	cb            func(w *FormatWriter) error
	formatOptions []FormatOption
}

type ExternalDataOption interface {
	applyExternalDataOption(*externalData)
}

func WithExternalData(name string, opts ...ExternalDataOption) externalData {
	ret := externalData{
		name: name,
	}

	for _, opt := range opts {
		opt.applyExternalDataOption(&ret)
	}

	return ret
}

func (o externalData) applySelectOptions(opts *selectOptions) {
	opts.externalData = append(opts.externalData, o)
}
