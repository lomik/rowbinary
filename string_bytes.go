package rowbinary

import (
	"encoding/binary"
)

var StringBytes Type[[]byte] = MakeTypeWrapAny[[]byte](typeStringBytes{})

type typeStringBytes struct{}

func (t typeStringBytes) String() string {
	return "String"
}

func (t typeStringBytes) Binary() []byte {
	return BinaryTypeString[:]
}

func (t typeStringBytes) Write(w Writer, value []byte) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = w.Write(value)
	return err
}

func (t typeStringBytes) Scan(r Reader, v *[]byte) (err error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}

	*v = (*v)[:0]

	p, err := r.Peek(int(n))
	if err != nil {
		return err
	}

	*v = append(*v, p...)
	_, err = r.Discard(int(n))
	return err
}
