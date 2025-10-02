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
	return r.ReadByte()
}

func (t typeUInt8) Scan(r Reader, v *uint8) (err error) {
	*v, err = t.Read(r)
	return
}
