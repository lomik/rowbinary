package rowbinary

import (
	"bytes"
	"testing"

	"github.com/shopspring/decimal"
)

func TestDecimalRejectOverflow(t *testing.T) {
	for _, tc := range []struct {
		tp     Type[decimal.Decimal]
		values []string
	}{
		{Decimal32(9, 0), []string{"4294967296", "-4294967296", "1000000000", "-1000000000"}},
		{Decimal64(18, 0), []string{"18446744073709551616", "-18446744073709551616", "1000000000000000000", "-1000000000000000000"}},
		{Decimal32(3, 2), []string{"10", "-10"}},
	} {
		for _, s := range tc.values {
			t.Run(tc.tp.String()+"/"+s, func(t *testing.T) {
				var buf bytes.Buffer
				if err := tc.tp.Write(NewWriter(&buf), decimal.RequireFromString(s)); err == nil {
					t.Fatal("overflow accepted")
				}
				if buf.Len() != 0 {
					t.Fatal("overflow wrote partial data")
				}
			})
		}
	}
}
