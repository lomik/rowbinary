package rowbinary

import "io"

type byteWriter interface {
	io.Writer
	io.ByteWriter
}

type writer struct {
	byteWriter
	buf [16]byte
}

type Writer interface {
	io.Writer
	io.ByteWriter
	buffer() []byte // 16 bytes buffer for encoding
}

type implByteWriter struct {
	io.Writer
	b []byte
}

func NewWriter(w io.Writer) Writer {
	return &writer{
		byteWriter: newByteWriter(w),
	}
}

func (w *writer) buffer() []byte {
	return w.buf[:]
}

func newByteWriter(w io.Writer) byteWriter {
	if bw, ok := w.(byteWriter); ok {
		return bw
	}
	return &implByteWriter{
		Writer: w,
		b:      make([]byte, 1),
	}
}

func (bw *implByteWriter) WriteByte(b byte) error {
	bw.b[0] = b
	_, err := bw.Write(bw.b)
	return err
}
