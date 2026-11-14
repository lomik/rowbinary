package rowbinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestDateTime64WideRange(t *testing.T) {
	for p := uint8(0); p <= 9; p++ {
		for _, zone := range []string{"UTC", "Asia/Shanghai"} {
			loc, err := time.LoadLocation(zone)
			if err != nil {
				t.Fatal(err)
			}
			for _, tz := range []bool{false, true} {
				tp := DateTime64(p)
				if tz {
					tp = DateTime64TZ(p, zone)
				}
				for _, year := range []int{1900, 2023, 2299} {
					if p == 9 && year == 2299 {
						continue // Precision 9 ends in 2262, unlike precisions 0..8.
					}
					t.Run(fmt.Sprintf("%s/%s/%d", tp.String(), zone, year), func(t *testing.T) {
						scale := int64(1)
						for i := uint8(0); i < p; i++ {
							scale *= 10
						}
						unit := int64(time.Second) / scale
						nsec := int64(123456789) / unit * unit
						want := time.Date(year, 1, 1, 0, 0, 0, int(nsec), loc)
						ticks := want.Unix()*scale + nsec/unit
						var wire [8]byte
						binary.LittleEndian.PutUint64(wire[:], uint64(ticks))
						t.Run("write", func(t *testing.T) {
							var buf bytes.Buffer
							if err := tp.Write(NewWriter(&buf), want); err != nil {
								t.Fatal(err)
							}
							if !bytes.Equal(buf.Bytes(), wire[:]) {
								t.Fatalf("wire = %x, want %x", buf.Bytes(), wire)
							}
						})
						t.Run("scan", func(t *testing.T) {
							var got time.Time
							if err := tp.Scan(NewReader(bytes.NewReader(wire[:])), &got); err != nil {
								t.Fatal(err)
							}
							if !got.Equal(want) {
								t.Fatalf("got %s, want %s", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
							}
						})
					})
				}
			}
		}
	}
}
