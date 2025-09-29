package rowbinary

import (
	"bytes"
	"testing"
)

func BenchmarkEqBinaryDiff(b *testing.B) {
	t := Map(UInt32, String)
	for b.Loop() {
		if bytes.Equal(t.Binary(), UInt32.Binary()) {
			b.Fatal("not equal")
		}
	}
}

func BenchmarkEqBinarySame(b *testing.B) {
	t := Map(UInt32, String)
	for b.Loop() {
		if !bytes.Equal(t.Binary(), t.Binary()) {
			b.Fatal("not equal")
		}
	}
}

func BenchmarkEqIDDiff(b *testing.B) {
	t := Map(UInt32, String)
	for b.Loop() {
		if Eq(t, UInt32) {
			b.Fatal("not equal")
		}
	}
}

func BenchmarkEqIDSame(b *testing.B) {
	t := Map(UInt32, String)
	for b.Loop() {
		if !Eq(t, t) {
			b.Fatal("not equal")
		}
	}
}
