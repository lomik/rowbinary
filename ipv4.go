package rowbinary

import "io"

var IPv4 Type[[4]byte] = MakeTypeWrapAny[[4]byte](typeIPv4{})

type typeIPv4 struct{}

func (t typeIPv4) String() string {
	return "IPv4"
}

func (t typeIPv4) Binary() []byte {
	return BinaryTypeIPv4[:]
}

func (t typeIPv4) Write(w Writer, value [4]byte) error {
	var ret [4]byte
	copy(ret[:], value[:])
	swap32(ret[:])
	_, err := w.Write(ret[:])
	return err
}

func (t typeIPv4) Read(r Reader) ([4]byte, error) {
	var ret [4]byte
	_, err := io.ReadAtLeast(r, ret[:], 4)
	swap32(ret[:])
	return ret, err
}

func (t typeIPv4) Scan(r Reader, v *[4]byte) (err error) {
	_, err = io.ReadAtLeast(r, (*v)[:], 4)
	swap32((*v)[:])
	return
}
