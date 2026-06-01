package rowbinary

import (
	"bufio"
	"encoding/binary"
	"errors"
	"io"
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

	// Fast path: the value fits in the reader's buffer.
	p, err := r.Peek(int(n))
	if err == nil {
		*v = append((*v)[:0], p...)
		_, err = r.Discard(int(n))
		return err
	}
	if !errors.Is(err, bufio.ErrBufferFull) {
		return err
	}

	// Slow path: the value is larger than the reader's buffer; read it directly,
	// reusing the destination's capacity when possible.
	if cap(*v) >= int(n) {
		*v = (*v)[:n]
	} else {
		*v = make([]byte, n)
	}
	_, err = io.ReadFull(r, *v)
	return err
}
