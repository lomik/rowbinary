package rowbinary

import (
	"bufio"
	"io"
	"testing"
)

func BenchmarkStringWrite(b *testing.B) {
	out := bufio.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		String.Write(out, "hello world")
	}
}
