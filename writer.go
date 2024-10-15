package rowbinary

import "io"

type OriginWriter interface {
	io.Writer
	io.ByteWriter
}

type typeWriter struct {
	OriginWriter
	buffer [16]byte
}

func NewWriter(wrap OriginWriter) Writer {
	return &typeWriter{
		OriginWriter: wrap,
	}
}

func (w *typeWriter) Buffer() []byte {
	return w.buffer[:]
}
