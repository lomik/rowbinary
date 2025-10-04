package rowbinary

var Int64 Type[int64] = MakeTypeWrapAny[int64](typeInt64{})

type typeInt64 struct{}

func (t typeInt64) String() string {
	return "Int64"
}

func (t typeInt64) Binary() []byte {
	return BinaryTypeInt64[:]
}

func (t typeInt64) Write(w Writer, value int64) error {
	return UInt64.Write(w, uint64(value))
}

func (t typeInt64) Scan(r Reader, v *int64) (err error) {
	var u uint64
	err = UInt64.Scan(r, &u)
	*v = int64(u)
	return err
}
