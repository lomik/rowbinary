package rowbinary

import "testing"

func TestQuoteIdentifier(t *testing.T) {
	for _, tc := range []struct {
		parts []string
		want  string
	}{
		{[]string{"events"}, "`events`"},
		{[]string{"db", "events"}, "`db`.`events`"},
		{[]string{"a.b"}, "`a.b`"},
		{[]string{"we`ird"}, "`we\\`ird`"},
		{[]string{`back\slash`}, "`back\\\\slash`"},
		{[]string{"it's"}, "`it's`"},
		{[]string{"line\nbreak"}, "`line\\nbreak`"},
		{[]string{"x; DROP TABLE y"}, "`x; DROP TABLE y`"},
	} {
		if got := QuoteIdentifier(tc.parts...); got != tc.want {
			t.Errorf("QuoteIdentifier(%q) = %s, want %s", tc.parts, got, tc.want)
		}
	}
}
