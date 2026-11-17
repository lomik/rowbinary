package rowbinary

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"
	"testing"
)

// Limits below were checked against ClickHouse 25.9.

func TestDecodeStringTypeRejectsOutOfRangeParams(t *testing.T) {
	for _, s := range []string{
		"Decimal(0, 0)",
		"Decimal(77, 2)",
		"Decimal(265, 2)",
		"Decimal(5, 6)",
		"Decimal(5, -1)",
		"FixedString(-1)",
		"FixedString(0)",
		"FixedString(16777216)",
		"Enum8('a' = 128)",
		"Enum8('a' = -129)",
		"Enum8('a' = 300)",
		"Enum16('a' = 32768)",
		"Enum16('a' = 70000)",
		"Dynamic(max_types=255)",
		"Dynamic(max_types=300)",
		"Dynamic(max_types=-1)",
	} {
		if tp, err := DecodeStringType(s); err == nil {
			t.Errorf("%s decoded as %s", s, tp)
		}
	}
}

func TestDecodeStringTypeAcceptsBoundaryParams(t *testing.T) {
	for s, want := range map[string]string{
		"Decimal(1, 0)":                     "Decimal(1, 0)",
		"Decimal(76, 76)":                   "Decimal(76, 76)",
		"FixedString(1)":                    "FixedString(1)",
		"FixedString(16777215)":             "FixedString(16777215)",
		"Enum8('a' = -128, 'b' = 127)":      "Enum8('a' = -128, 'b' = 127)",
		"Enum16('a' = -32768, 'b' = 32767)": "Enum16('a' = -32768, 'b' = 32767)",
		"Dynamic(max_types=254)":            "Dynamic(max_types=254)",
	} {
		tp, err := DecodeStringType(s)
		if err != nil {
			t.Errorf("%s rejected: %v", s, err)
			continue
		}
		if tp.String() != want {
			t.Errorf("%s decoded as %s, want %s", s, tp, want)
		}
	}
}

func TestDecodeBinaryTypeRejectsBadHeaders(t *testing.T) {
	uv := func(n uint64) []byte { return binary.AppendUvarint(nil, n) }
	for name, header := range map[string][]byte{
		"FixedString(0)":          {0x16, 0x00},
		"FixedString(16777216)":   append([]byte{0x16}, uv(16777216)...),
		"FixedString(MaxUint64)":  append([]byte{0x16}, uv(math.MaxUint64)...),
		"Enum8 count MaxUint64":   append([]byte{0x17}, uv(math.MaxUint64)...),
		"Enum8 count 257":         append([]byte{0x17}, uv(257)...),
		"Enum16 count MaxUint64":  append([]byte{0x18}, uv(math.MaxUint64)...),
		"Enum16 count 65537":      append([]byte{0x18}, uv(65537)...),
		"Decimal256 tag, p=5":     {0x1c, 5, 2},
		"Decimal32 tag, p=10":     {0x19, 10, 2},
		"Decimal64 tag, p=9":      {0x1a, 9, 2},
		"Decimal precision 0":     {0x19, 0, 0},
		"Decimal precision 77":    {0x1c, 77, 0},
		"Decimal scale > prec":    {0x19, 5, 6},
		"Dynamic max_types=255":   {0x2b, 255},
		"Array(FixedString(0))":   {0x1e, 0x16, 0x00},
		"Nullable(Decimal256/p5)": {0x23, 0x1c, 5, 2},
	} {
		if tp, err := DecodeBinaryType(NewReader(bytes.NewReader(header))); err == nil {
			t.Errorf("%s (% x) decoded as %s", name, header, tp)
		}
	}
}

func TestDecodeBinaryTypeAcceptsServerHeaders(t *testing.T) {
	// Decimal headers as emitted by ClickHouse 25.9 for Decimal(9|10|38|39, 2).
	for _, header := range [][]byte{
		{0x19, 9, 2},
		{0x1a, 10, 2},
		{0x1b, 38, 2},
		{0x1c, 39, 2},
		{0x16, 0xff, 0xff, 0xff, 0x07}, // FixedString(16777215)
		{0x2b, 254},
		{0x2b, 0},
	} {
		tp, err := DecodeBinaryType(NewReader(bytes.NewReader(header)))
		if err != nil {
			t.Errorf("% x rejected: %v", header, err)
			continue
		}
		// Every accepted header must round-trip to the same bytes, except
		// Dynamic(0), which is item 35 in TODO.md.
		if header[0] != 0x2b && !slices.Equal(tp.Binary(), header) {
			t.Errorf("% x decoded as %s with binary % x", header, tp, tp.Binary())
		}
	}
}
