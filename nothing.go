package rowbinary

var Nothing Type[any] = MakeTypeWrapAny[any](typeNothing{})

type typeNothing struct{}

func (t typeNothing) String() string {
	return "Nothing"
}

func (t typeNothing) Binary() []byte {
	return BinaryTypeNothing[:]
}

func (t typeNothing) Write(w Writer, value any) error {
	return nil
}

func (t typeNothing) Read(r Reader) (any, error) {
	return nil, nil
}

func (t typeNothing) Scan(r Reader, v *any) (err error) {
	*v, err = t.Read(r)
	return
}
