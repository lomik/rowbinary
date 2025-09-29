package rowbinary

import (
	"encoding/binary"
	"errors"
	"io"
)

var UInt64 Type[uint64] = typeUInt64{}

type typeUInt64 struct{}

var typeUInt64ID = BinaryTypeID(BinaryTypeUInt64[:])

func (t typeUInt64) String() string {
	return "UInt64"
}

func (t typeUInt64) Binary() []byte {
	return BinaryTypeUInt64[:]
}

func (t typeUInt64) ID() uint64 {
	return typeUInt64ID
}

func (t typeUInt64) Write(w Writer, value uint64) error {
	binary.LittleEndian.PutUint64(w.buffer(), value)
	_, err := w.Write(w.buffer()[:8])
	return err
}

func readAtLeast(r Reader, buf []byte, min int) (int, error) {
	n, err := r.Read(buf)
	if n == min {
		return n, nil
	}

	for n < min && err == nil {
		var nn int
		nn, err = r.Read(buf[n:])
		n += nn
	}
	if n >= min {
		err = nil
	} else if n > 0 && err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}

func (t typeUInt64) Read(r Reader) (uint64, error) {
	b, err := r.Peek(8)
	if err != nil {
		return 0, err
	}
	ret := binary.LittleEndian.Uint64(b)
	r.Discard(8)
	return ret, nil
}

func (t typeUInt64) WriteAny(w Writer, v any) error {
	value, ok := v.(uint64)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeUInt64) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
