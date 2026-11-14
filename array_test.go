package rowbinary

import (
	"bytes"
	"fmt"
	"testing"
)

func TestArrayLengthOverflow(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		for _, anyType := range []bool{false, true} {
			t.Run(fmt.Sprintf("reuse=%t/any=%t", reuse, anyType), func(t *testing.T) {
				defer func() {
					if p := recover(); p != nil {
						t.Fatalf("length overflow panicked: %v", p)
					}
				}()
				r := NewReader(bytes.NewReader(VarintEncode(^uint64(0))))
				var err error
				if anyType {
					var v []any
					if reuse {
						v = []any{uint32(42)}
					}
					err = ArrayAny(UInt32).Scan(r, &v)
				} else {
					var v []uint32
					if reuse {
						v = []uint32{42}
					}
					err = Array(UInt32).Scan(r, &v)
				}
				if err == nil {
					t.Fatal("expected invalid length error")
				}
			})
		}
	}
}
