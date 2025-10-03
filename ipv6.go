package rowbinary

import "io"

var IPv6 Type[[16]byte] = MakeTypeWrapAny[[16]byte](typeIPv6{})

type typeIPv6 struct{}

func (t typeIPv6) String() string {
	return "IPv6"
}

func (t typeIPv6) Binary() []byte {
	return BinaryTypeIPv6[:]
}

func (t typeIPv6) Write(w Writer, value [16]byte) error {
	_, err := w.Write(value[:])
	return err
}

func (t typeIPv6) Read(r Reader) ([16]byte, error) {
	var ret [16]byte
	_, err := io.ReadAtLeast(r, ret[:], 16)
	return ret, err
}

func (t typeIPv6) Scan(r Reader, v *[16]byte) (err error) {
	_, err = io.ReadAtLeast(r, (*v)[:], 16)
	return
}
