package rowbinary

import (
	"bytes"
	"fmt"
	"testing"
)

func TestDecodeBinaryTypeHugeCounts(t *testing.T) {
	for _, tag := range [][1]byte{BinaryTypeTuple, BinaryTypeTupleNamed, BinaryTypeVariant, BinaryTypeNested} {
		t.Run(fmt.Sprintf("%x", tag), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("count caused panic: %v", p)
				}
			}()
			wire := append([]byte{tag[0]}, VarintEncode(^uint64(0))...)
			if _, err := DecodeBinaryType(NewReader(bytes.NewReader(wire))); err == nil {
				t.Fatal("expected error for missing declared elements")
			}
		})
	}
}
