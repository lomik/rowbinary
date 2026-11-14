package rowbinary

import (
	"bytes"
	"fmt"
	"testing"
)

func TestStringHugeLength(t *testing.T) {
	for _, n := range []uint64{uint64(^uint(0) >> 1), ^uint64(0)} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			defer func() {
				if p := recover(); p != nil {
					t.Fatalf("string length caused panic: %v", p)
				}
			}()
			value := "unchanged"
			if err := String.Scan(NewReader(bytes.NewReader(VarintEncode(n))), &value); err == nil {
				t.Fatal("expected error for missing string data")
			}
			if value != "unchanged" {
				t.Fatal("failed scan changed destination")
			}
		})
	}
}
