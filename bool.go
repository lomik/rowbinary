package rowbinary

var Bool Type[bool] = MakeTypeWrapAny[bool](typeBool{})

type typeBool struct{}

func (t typeBool) String() string {
	return "Bool"
}

func (t typeBool) Binary() []byte {
	return BinaryTypeBool[:]
}

func (t typeBool) Write(w Writer, value bool) error {
	if value {
		return UInt8.Write(w, 1)
	}
	return UInt8.Write(w, 0)
}

func (t typeBool) Scan(r Reader, v *bool) error {
	val, err := r.ReadByte()
	if err != nil {
		return err
	}
	// Any non-zero byte is true, as in ClickHouse (which stores and returns
	// e.g. 255 as is).
	*v = val != 0
	return nil
}
