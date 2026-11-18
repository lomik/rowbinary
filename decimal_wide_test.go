package rowbinary

import (
	"bytes"
	"encoding/hex"
	"io"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

// decimalWideCases are RowBinary encodings produced by ClickHouse 25.9 with
// SELECT CAST(arrayJoin([...]) AS Decimal(p, s)) FORMAT RowBinary.
var decimalWideCases = []struct {
	precision, scale uint8
	value, hex       string
}{
	{19, 0, "0", "00000000000000000000000000000000"},
	{19, 0, "1", "01000000000000000000000000000000"},
	{19, 0, "-1", "ffffffffffffffffffffffffffffffff"},
	{19, 0, "9999999999999999999", "ffffe7890423c78a0000000000000000"},
	{19, 0, "-9999999999999999999", "01001876fbdc3875ffffffffffffffff"},
	{38, 2, "0", "00000000000000000000000000000000"},
	{38, 2, "4.2", "a4010000000000000000000000000000"},
	{38, 2, "-4.2", "5cfeffffffffffffffffffffffffffff"},
	{38, 2, "0.01", "01000000000000000000000000000000"},
	{38, 2, "-0.01", "ffffffffffffffffffffffffffffffff"},
	{38, 2, "999999999999999999999999999999999999.99", "ffffffff3f228a097ac4865aa84c3b4b"},
	{38, 2, "-999999999999999999999999999999999999.99", "01000000c0dd75f6853b79a557b3c4b4"},
	{38, 2, "92233720368547758.08", "00000000000000800000000000000000"},
	{38, 2, "-92233720368547758.09", "ffffffffffffff7fffffffffffffffff"},
	{38, 38, "0.5", "000000002011c5043d62432d54a69d25"},
	{38, 38, "-0.5", "00000000e0ee3afbc29dbcd2ab5962da"},
	{38, 38, "0.99999999999999999999999999999999999999", "ffffffff3f228a097ac4865aa84c3b4b"},
	{39, 0, "1", "0100000000000000000000000000000000000000000000000000000000000000"},
	{39, 0, "-1", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	{39, 0, "999999999999999999999999999999999999999", "ffffffff7f56655fc4ac438993fe50f002000000000000000000000000000000"},
	{39, 0, "-999999999999999999999999999999999999999", "0100000080a99aa03b53bc766c01af0ffdffffffffffffffffffffffffffffff"},
	{76, 10, "0", "0000000000000000000000000000000000000000000000000000000000000000"},
	{76, 10, "-1.5", "002aee81fcffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	{76, 10, "123456789012345678901234567890.0123456789", "15d5048c965fbcacb8f3dbc07520c9a003000000000000000000000000000000"},
	{76, 10, "999999999999999999999999999999999999999999999999999999999999999999.9999999999", "ffffffffffffffffff0f9571f1a57577792965e8abb46407b5159911a7cc1b16"},
	{76, 10, "-999999999999999999999999999999999999999999999999999999999999999999.9999999999", "010000000000000000f06a8e0e5a8a8886d69a17544b9bf84aea66ee5833e4e9"},
	{76, 76, "0.1", "000000000000000000e88ebe312af28bf2503d977778f0b32b82c281ddfa3502"},
	{76, 76, "-0.1", "000000000000000000187141ced50d740dafc26888870f4cd47d3d7e2205cafd"},
}

func TestDecimalWideMatchesClickHouse(t *testing.T) {
	for _, tc := range decimalWideCases {
		tp := Decimal(tc.precision, tc.scale)
		want, _ := hex.DecodeString(tc.hex)
		value := decimal.RequireFromString(tc.value)

		var buf bytes.Buffer
		if err := tp.Write(NewWriter(&buf), value); err != nil {
			t.Errorf("%s Write(%s): %v", tp, tc.value, err)
			continue
		}
		if !bytes.Equal(buf.Bytes(), want) {
			t.Errorf("%s Write(%s) = %x, want %s", tp, tc.value, buf.Bytes(), tc.hex)
		}

		var got decimal.Decimal
		if err := tp.Scan(NewReader(bytes.NewReader(want)), &got); err != nil {
			t.Errorf("%s Scan(%s): %v", tp, tc.hex, err)
			continue
		}
		if !got.Equal(value) {
			t.Errorf("%s Scan(%s) = %s, want %s", tp, tc.hex, got, tc.value)
		}
	}
}

func TestDecimalWideRejectsOverflowAndTruncates(t *testing.T) {
	for _, tc := range []struct {
		precision, scale uint8
		value            string
	}{
		{38, 2, "1" + strings.Repeat("0", 36)},
		{38, 2, "-1" + strings.Repeat("0", 36)},
		{19, 0, "10000000000000000000"},
		{76, 0, "1" + strings.Repeat("0", 76)},
		{76, 76, "1"},
		{76, 76, "-1"},
	} {
		tp := Decimal(tc.precision, tc.scale)
		var buf bytes.Buffer
		if err := tp.Write(NewWriter(&buf), decimal.RequireFromString(tc.value)); err == nil {
			t.Errorf("%s accepted %s", tp, tc.value)
		}
		if buf.Len() != 0 {
			t.Errorf("%s wrote %d bytes for a rejected value", tp, buf.Len())
		}
	}

	// Extra fractional digits are truncated toward zero, as for Decimal32/64.
	for in, want := range map[string]string{"1.239": "1.23", "-1.239": "-1.23"} {
		tp := Decimal(38, 2)
		var buf bytes.Buffer
		if err := tp.Write(NewWriter(&buf), decimal.RequireFromString(in)); err != nil {
			t.Fatal(err)
		}
		var got decimal.Decimal
		if err := tp.Scan(NewReader(&buf), &got); err != nil || !got.Equal(decimal.RequireFromString(want)) {
			t.Errorf("Write(%s) then Scan = %s, %v; want %s", in, got, err, want)
		}
	}
}

func TestDecimalWideTruncatedInput(t *testing.T) {
	for _, tp := range []Type[decimal.Decimal]{Decimal(38, 2), Decimal(76, 2)} {
		var got decimal.Decimal
		if err := tp.Scan(NewReader(bytes.NewReader(make([]byte, 15))), &got); err == nil {
			t.Errorf("%s scanned 15 bytes", tp)
		}
		if err := tp.Write(NewWriter(io.Discard), decimal.NewFromInt(1)); err != nil {
			t.Errorf("%s: %v", tp, err)
		}
	}
}
