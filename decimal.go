package rowbinary

import (
	"github.com/shopspring/decimal"
)

func Decimal(precision uint8, scale uint8) Type[decimal.Decimal] {
	if precision <= 9 {
		return Decimal32(precision, scale)
	} else if precision <= 18 {
		return Decimal64(precision, scale)
	} else if precision <= 38 {
		return Decimal128(precision, scale)
	} else if precision <= 76 {
		return Decimal256(precision, scale)
	}
	return Invalid[decimal.Decimal]("Decimal precision must be in range 1..76")
}
