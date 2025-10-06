package rowbinary

type paramOption struct {
	name  string
	value string
}

type headerOption struct {
	name  string
	value string
}

type dsnOption struct {
	dsn string
}

// WithDSN sets the DSN for the request.
func WithDSN(dsn string) dsnOption {
	return dsnOption{dsn: dsn}
}

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

func (o dsnOption) applySelectOptions(opts *selectOptions) {
	opts.dsn = o.dsn
}

func (o dsnOption) applyInsertOptions(opts *insertOptions) {
	opts.dsn = o.dsn
}

func (o dsnOption) applyExecOptions(opts *execOptions) {
	opts.dsn = o.dsn
}

func (o dsnOption) applyClientOptions(opts *clientOptions) {
	opts.defaultSelect = append(opts.defaultSelect, o)
	opts.defaultInsert = append(opts.defaultInsert, o)
	opts.defaultExec = append(opts.defaultExec, o)
}
