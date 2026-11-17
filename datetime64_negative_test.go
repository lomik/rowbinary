package rowbinary

import (
	"bytes"
	"encoding/hex"
	"testing"
	"time"
)

// Encodings produced by ClickHouse 25.9 with
// SELECT toDateTime64(value, precision, 'UTC') FORMAT RowBinary.
var dateTime64ClickHouseCases = []struct {
	value     string
	precision uint8
	hex       string
}{
	{"1969-12-31 23:59:59.5", 1, "fbffffffffffffff"},
	{"1969-12-31 23:59:59.5", 3, "0cfeffffffffffff"},
	{"1969-12-31 23:59:59.999", 3, "ffffffffffffffff"},
	{"1969-12-31 23:59:59.000000001", 9, "013665c4ffffffff"},
	{"1969-12-31 23:59:59.999999999", 9, "ffffffffffffffff"},
	{"1900-01-01 00:00:00.123", 3, "7bdc01aefdfdffff"},
	{"1950-06-15 12:30:45.654321", 6, "31a3fc31facefdff"},
	{"1970-01-01 00:00:00.5", 3, "f401000000000000"},
}

func TestDateTime64NegativeFractionalMatchesClickHouse(t *testing.T) {
	for _, tc := range dateTime64ClickHouseCases {
		want, _ := hex.DecodeString(tc.hex)
		value, err := time.Parse("2006-01-02 15:04:05.999999999", tc.value)
		if err != nil {
			t.Fatal(err)
		}
		for _, tp := range []Type[time.Time]{DateTime64(tc.precision), DateTime64TZ(tc.precision, "UTC")} {
			var buf bytes.Buffer
			if err := tp.Write(NewWriter(&buf), value); err != nil {
				t.Errorf("%s Write(%s): %v", tp, tc.value, err)
				continue
			}
			if !bytes.Equal(buf.Bytes(), want) {
				t.Errorf("%s Write(%s) = %x, want %s", tp, tc.value, buf.Bytes(), tc.hex)
			}
			var got time.Time
			if err := tp.Scan(NewReader(bytes.NewReader(want)), &got); err != nil || !got.Equal(value) {
				t.Errorf("%s Scan(%s) = %s, %v; want %s", tp, tc.hex, got, err, tc.value)
			}
		}
	}
}
