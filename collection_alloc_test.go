package rowbinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"runtime"
	"testing"
)

// A few bytes can declare a huge collection; decoders must not allocate for
// the declared count before the data is actually there.
func TestHugeDeclaredCollectionsDoNotPreallocate(t *testing.T) {
	const declared = 1 << 40
	input := append(binary.AppendUvarint(nil, declared), 1, 2, 3, 4)

	for name, scan := range map[string]func(r Reader) error{
		"Array(UInt8)":       func(r Reader) error { var v []uint8; return Array(UInt8).Scan(r, &v) },
		"ArrayAny(UInt8)":    func(r Reader) error { var v []any; return ArrayAny(UInt8).Scan(r, &v) },
		"MapAny(UInt8, ...)": func(r Reader) error { var v map[any]any; return MapAny(UInt8, UInt8).Scan(r, &v) },
		"Map(UInt8, UInt8)":  func(r Reader) error { var v map[uint8]uint8; return Map(UInt8, UInt8).Scan(r, &v) },
		"MapKV(UInt8, UInt8)": func(r Reader) error {
			var v *KV[uint8, uint8]
			return MapKV(UInt8, UInt8).Scan(r, &v)
		},
	} {
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		err := scan(NewReader(bytes.NewReader(input)))
		runtime.ReadMemStats(&after)
		if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Errorf("%s: err = %v, want end of input", name, err)
		}
		if grew := after.TotalAlloc - before.TotalAlloc; grew > 4<<20 {
			t.Errorf("%s allocated %d MiB for %d bytes of input", name, grew>>20, len(input))
		}
	}
}

func TestArrayScanReusesDestination(t *testing.T) {
	var buf bytes.Buffer
	if err := Array(UInt8).Write(NewWriter(&buf), []uint8{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	dst := make([]uint8, 0, 8)
	if err := Array(UInt8).Scan(NewReader(bytes.NewReader(buf.Bytes())), &dst); err != nil {
		t.Fatal(err)
	}
	if len(dst) != 3 || cap(dst) != 8 || dst[2] != 3 {
		t.Fatalf("dst = %v (cap %d), want [1 2 3] in the original backing array", dst, cap(dst))
	}
}
