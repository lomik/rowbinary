package rowbinary

import (
	"bufio"
	"io"
	"testing"
)

func BenchmarkFloat64(b *testing.B) {
	out := bufio.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Float64.Write(out, 42)
	}
}
