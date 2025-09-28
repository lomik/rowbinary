package rowbinary

var _ FormatOption = NewColumn("", UInt8)

type Column struct {
	name string
	tp   Any
}

// applyFormatOption implements FormatReaderOption.
func (c Column) applyFormatOption(o *formatOptions) {
	o.columns = append(o.columns, c)
}

func NewColumn(name string, tp Any) Column {
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
