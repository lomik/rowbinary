package rowbinary

import (
	"fmt"
	"math"
	"strconv"
)

// Type parameter limits enforced by ClickHouse (checked against server 25.9).
const (
	maxFixedStringSize = 0xFFFFFF // 16777215
	maxDynamicTypes    = 254
	maxDecimalPrec     = 76
	maxEnum8Elements   = math.MaxUint8 + 1
	maxEnum16Elements  = math.MaxUint16 + 1
)

// parseIntInRange parses a decimal integer type parameter and checks min <= v <= max.
func parseIntInRange(what, s string, min, max int64) (int64, error) {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("can't parse %s: %w", what, err)
	}
	if v < min || v > max {
		return 0, fmt.Errorf("%s must be in range %d..%d, got %d", what, min, max, v)
	}
	return v, nil
}

func checkFixedStringSize(n uint64) error {
	if n < 1 || n > maxFixedStringSize {
		return fmt.Errorf("FixedString size must be in range 1..%d, got %d", maxFixedStringSize, n)
	}
	return nil
}

func checkDecimalParams(precision, scale uint64) error {
	if precision < 1 || precision > maxDecimalPrec {
		return fmt.Errorf("Decimal precision must be in range 1..%d, got %d", maxDecimalPrec, precision)
	}
	if scale > precision {
		return fmt.Errorf("Decimal scale must be in range 0..%d, got %d", precision, scale)
	}
	return nil
}

// decimalTag returns the binary type tag ClickHouse uses for a Decimal of the given precision.
func decimalTag(precision uint8) byte {
	switch {
	case precision <= 9:
		return BinaryTypeDecimal32[0]
	case precision <= 18:
		return BinaryTypeDecimal64[0]
	case precision <= 38:
		return BinaryTypeDecimal128[0]
	default:
		return BinaryTypeDecimal256[0]
	}
}
