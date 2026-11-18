package rowbinary

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
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

	if n > uint64(^uint(0)>>1) {
		return fmt.Errorf("string length overflows int: %d", n)
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

	// Slow path: the value is larger than the reader's buffer. Reuse the
	// destination's capacity, but grow only as data arrives, never from an
	// untrusted declared size.
	b := (*v)[:0]
	var chunk [4096]byte
	for remaining := n; remaining > 0; {
		size := min(remaining, uint64(len(chunk)))
		if _, err = io.ReadFull(r, chunk[:size]); err != nil {
			*v = b
			return err
		}
		b = append(b, chunk[:size]...)
		remaining -= size
	}
	*v = b
	return nil
}
