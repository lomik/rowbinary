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
	*v = val == 1
	return nil
}
