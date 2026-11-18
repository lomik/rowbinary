package rowbinary

import (
	"io"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func TestSizedDecimalConstructorsValidateParams(t *testing.T) {
	for name, tp := range map[string]Type[decimal.Decimal]{
		"Decimal32(12, 2)":  Decimal32(12, 2),
		"Decimal32(0, 0)":   Decimal32(0, 0),
		"Decimal32(5, 6)":   Decimal32(5, 6),
		"Decimal64(5, 2)":   Decimal64(5, 2),
		"Decimal64(19, 2)":  Decimal64(19, 2),
		"Decimal128(18, 2)": Decimal128(18, 2),
		"Decimal128(39, 2)": Decimal128(39, 2),
		"Decimal256(38, 2)": Decimal256(38, 2),
		"Decimal256(77, 2)": Decimal256(77, 2),
		"Decimal(0, 0)":     Decimal(0, 0),
		"Decimal(10, 11)":   Decimal(10, 11),
	} {
		if tp.String() != "Invalid" {
			t.Errorf("%s = %s, want Invalid", name, tp)
			continue
		}
		if err := tp.Write(NewWriter(io.Discard), decimal.NewFromInt(1)); err == nil {
			t.Errorf("%s: Write accepted", name)
		}
	}
	if err := Decimal64(5, 2).Write(NewWriter(io.Discard), decimal.NewFromInt(1)); err == nil || !strings.Contains(err.Error(), "use Decimal(5, 2)") {
		t.Errorf("Decimal64(5, 2) error = %v", err)
	}

	// Each constructor accepts its own precision range, at both ends.
	for name, tp := range map[string]Type[decimal.Decimal]{
		"Decimal32(1, 0)":     Decimal32(1, 0),
		"Decimal32(9, 9)":     Decimal32(9, 9),
		"Decimal64(10, 0)":    Decimal64(10, 0),
		"Decimal64(18, 18)":   Decimal64(18, 18),
		"Decimal128(19, 0)":   Decimal128(19, 0),
		"Decimal128(38, 38)":  Decimal128(38, 38),
		"Decimal256(39, 0)":   Decimal256(39, 0),
		"Decimal256(76, 76)":  Decimal256(76, 76),
		"Decimal(5, 2) -> 32": Decimal(5, 2),
	} {
		if tp.String() == "Invalid" {
			t.Errorf("%s rejected", name)
		}
	}
}
