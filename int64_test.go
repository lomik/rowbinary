package rowbinary

import (
	"bufio"
	"io"
	"testing"
)

func BenchmarkInt64(b *testing.B) {
	out := bufio.NewWriter(io.Discard)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		Int64.Write(out, 42)
	}
}
