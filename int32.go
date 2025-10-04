package rowbinary

var Int32 Type[int32] = MakeTypeWrapAny[int32](typeInt32{})

type typeInt32 struct{}

func (t typeInt32) String() string {
	return "Int32"
}

func (t typeInt32) Binary() []byte {
	return BinaryTypeInt32[:]
}

func (t typeInt32) Write(w Writer, value int32) error {
	return UInt32.Write(w, uint32(value))
}

func (t typeInt32) Scan(r Reader, v *int32) (err error) {
	var u uint32
	err = UInt32.Scan(r, &u)
	*v = int32(u)
	return err
}
