package rowbinary

var UInt8 Type[uint8] = MakeTypeWrapAny[uint8](typeUInt8{})

type typeUInt8 struct{}

func (t typeUInt8) String() string {
	return "UInt8"
}

func (t typeUInt8) Binary() []byte {
	return BinaryTypeUInt8[:]
}

func (t typeUInt8) Write(w Writer, value uint8) error {
	return w.WriteByte(value)
}

func (t typeUInt8) Read(r Reader) (uint8, error) {
	b, err := r.ReadByte()
	if err != nil {
		return 0, err
	}
	return b, nil
}
