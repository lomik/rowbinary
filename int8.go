package rowbinary

var Int8 Type[int8] = MakeTypeWrapAny[int8](typeInt8{})

type typeInt8 struct{}

func (t typeInt8) String() string {
	return "Int8"
}

func (t typeInt8) Binary() []byte {
	return BinaryTypeInt8[:]
}

func (t typeInt8) Write(w Writer, value int8) error {
	return UInt8.Write(w, uint8(value))
}

func (t typeInt8) Read(r Reader) (int8, error) {
	v, err := UInt8.Read(r)
	return int8(v), err
}

func (t typeInt8) Scan(r Reader, v *int8) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
