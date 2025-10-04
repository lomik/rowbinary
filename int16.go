package rowbinary

var Int16 Type[int16] = MakeTypeWrapAny[int16](typeInt16{})

type typeInt16 struct{}

func (t typeInt16) String() string {
	return "Int16"
}

func (t typeInt16) Binary() []byte {
	return BinaryTypeInt16[:]
}

func (t typeInt16) Write(w Writer, value int16) error {
	return UInt16.Write(w, uint16(value))
}

func (t typeInt16) Scan(r Reader, v *int16) (err error) {
	var u uint16
	err = UInt16.Scan(r, &u)
	*v = int16(u)
	return err
}
