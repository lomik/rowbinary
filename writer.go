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
	Buffer() []byte // 16 bytes buffer for encoding
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

func (w *writer) Buffer() []byte {
	return w.buf[:]
}

// Write enforces the io.Writer contract that a short write returns an error,
// as io.Copy and bufio do, so a misbehaving destination cannot silently drop
// data.
func (w *writer) Write(p []byte) (int, error) {
	n, err := w.byteWriter.Write(p)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	return n, err
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
	n, err := bw.Write(bw.b)
	if err == nil && n < 1 {
		err = io.ErrShortWrite
	}
	return err
}
