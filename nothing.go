package rowbinary

var Nothing Type[any] = typeNothing{}

type typeNothing struct{}

var typeNothingID = BinaryTypeID(BinaryTypeNothing[:])

func (t typeNothing) String() string {
	return "Nothing"
}

func (t typeNothing) Binary() []byte {
	return BinaryTypeNothing[:]
}

func (t typeNothing) ID() uint64 {
	return typeNothingID
}

func (t typeNothing) Write(w Writer, value any) error {
	return nil
}

func (t typeNothing) Read(r Reader) (any, error) {
	return nil, nil
}

func (t typeNothing) WriteAny(w Writer, v any) error {
	return nil
}

func (t typeNothing) ReadAny(r Reader) (any, error) {
	return nil, nil
}
