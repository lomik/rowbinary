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

func (t typeInt64) Read(r Reader) (int64, error) {
	v, err := UInt64.Read(r)
	return int64(v), err
}

func (t typeInt64) Scan(r Reader, v *int64) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
