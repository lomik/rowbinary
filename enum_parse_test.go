package rowbinary

import (
	"testing"
)

// Labels with separators, brackets and escapes. serverEnum8 is the exact
// toTypeName output of ClickHouse 25.9 for this enum.
var trickyEnumLabels = map[string]int8{
	"a, b": 1, "c (d)": 2, "e f": 3, "it's": 4, `x\y`: 5, "nl\n": 6, "tab\t": 7, "q=1": 8,
}

const serverEnum8 = `Enum8('a, b' = 1, 'c (d)' = 2, 'e f' = 3, 'it\'s' = 4, 'x\\y' = 5, 'nl\n' = 6, 'tab\t' = 7, 'q=1' = 8)`

func TestEnumStringMatchesClickHouse(t *testing.T) {
	if got := Enum8(trickyEnumLabels).String(); got != serverEnum8 {
		t.Fatalf("Enum8.String()\n got %s\nwant %s", got, serverEnum8)
	}
}

func TestDecodeStringTypeEnumWithTrickyLabels(t *testing.T) {
	want := Enum8(trickyEnumLabels)
	tp, err := DecodeStringType(serverEnum8)
	if err != nil {
		t.Fatal(err)
	}
	if !Eq(tp, want) {
		t.Fatalf("decoded %s, want %s", tp, want)
	}

	// Enum16 and Enum inside a compound type.
	e16 := map[string]int16{"a, b": -1000, "it's": 1000}
	tp, err = DecodeStringType("Array(Nullable(" + Enum16(e16).String() + "))")
	if err != nil {
		t.Fatal(err)
	}
	if !Eq(tp, ArrayAny(NullableAny(Enum16(e16)))) {
		t.Fatalf("decoded %s", tp)
	}
}

func TestDecodeStringTypeEnumRejectsMalformedElements(t *testing.T) {
	for _, s := range []string{
		"Enum8(a = 1)",
		"Enum8('a' 1)",
		"Enum8('a' = )",
		"Enum8('a' = 1 2)",
	} {
		if tp, err := DecodeStringType(s); err == nil {
			t.Errorf("%s decoded as %s", s, tp)
		}
	}
}

func TestDateTimeTZQuotedNameRoundTrip(t *testing.T) {
	for _, s := range []string{"DateTime('Asia/Shanghai')", "DateTime64(3, 'UTC')"} {
		tp, err := DecodeStringType(s)
		if err != nil {
			t.Fatal(err)
		}
		if tp.String() != s {
			t.Errorf("%s -> %s", s, tp)
		}
	}
}
