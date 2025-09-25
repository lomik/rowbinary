package rowbinary

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

func BenchmarkTypes(b *testing.B) {
	for _, tt := range commonTestData {
		tt := tt

		b.Run(fmt.Sprintf("Write %s", tt.tp.String()), func(b *testing.B) {
			out := NewWriter(io.Discard)
			for i := 0; i < b.N; i++ {
				tt.tp.WriteAny(out, tt.want)
			}
		})
	}

	for _, tt := range commonTestData {
		tt := tt

		b.Run(fmt.Sprintf("Read %s", tt.tp.String()), func(b *testing.B) {
			buf := new(bytes.Buffer)
			data := buf.Bytes()
			br := bytes.NewReader(data)
			r := NewReader(br)
			for i := 0; i < b.N; i++ {
				br.Reset(data)
				tt.tp.ReadAny(r)
			}
		})
	}
}
