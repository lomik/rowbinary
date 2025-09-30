package rowbinary

var _ FormatOption = WithColumn("", UInt8)

type Column struct {
	name string
	tp   Any
}

// applyFormatOption implements FormatReaderOption.
func (c Column) applyFormatOption(o *formatOptions) {
	o.columns = append(o.columns, c)
}

func WithColumn(name string, tp Any) Column {
	return Column{
		name: name,
		tp:   tp,
	}
}

func C(name string, tp Any) Column {
	return Column{
		name: name,
		tp:   tp,
	}
}

func (c Column) applySelectOptions(o *selectOptions) {
	o.formatOptions = append(o.formatOptions, c)
}

func (c Column) applyInsertOptions(o *insertOptions) {
	o.formatOptions = append(o.formatOptions, c)
}

func (c Column) applyExternalDataOption(o *externalData) {
	o.formatOptions = append(o.formatOptions, c)
}

func (c Column) Name() string {
	return c.name
}

func (c Column) Type() Any {
	return c.tp
}

func (c Column) String() string {
	return c.Name() + " " + c.Type().String()
}
