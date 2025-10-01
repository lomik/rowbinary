package rowbinary

type paramOption struct {
	name  string
	value string
}

type headerOption struct {
	name  string
	value string
}

var _ SelectOption = WithParam("", "")
var _ InsertOption = WithParam("", "")
var _ ExecOption = WithParam("", "")

var _ SelectOption = WithHeader("", "")
var _ InsertOption = WithHeader("", "")
var _ ExecOption = WithHeader("", "")

func WithHeader(name, value string) headerOption {
	return headerOption{name: name, value: value}
}

func WithParam(name, value string) paramOption {
	return paramOption{name: name, value: value}
}

func (o paramOption) applySelectOptions(opts *selectOptions) {
	opts.params[o.name] = o.value
}

func (o paramOption) applyInsertOptions(opts *insertOptions) {
	opts.params[o.name] = o.value
}

func (o paramOption) applyExecOptions(opts *execOptions) {
	opts.params[o.name] = o.value
}

func (f paramOption) applyClientOptions(opts *clientOptions) {
	opts.defaultSelect = append(opts.defaultSelect, f)
	opts.defaultInsert = append(opts.defaultInsert, f)
	opts.defaultExec = append(opts.defaultExec, f)
}

func (o headerOption) applySelectOptions(opts *selectOptions) {
	opts.headers[o.name] = o.value
}

func (o headerOption) applyInsertOptions(opts *insertOptions) {
	opts.headers[o.name] = o.value
}

func (o headerOption) applyExecOptions(opts *execOptions) {
	opts.headers[o.name] = o.value
}

func (f headerOption) applyClientOptions(opts *clientOptions) {
	opts.defaultSelect = append(opts.defaultSelect, f)
	opts.defaultInsert = append(opts.defaultInsert, f)
	opts.defaultExec = append(opts.defaultExec, f)
}
