package rowbinary

import (
	"fmt"

	"github.com/shopspring/decimal"
)

func Decimal128(precision uint8, scale uint8) Type[decimal.Decimal] {
	return MakeTypeWrapAny(typeDecimal128{
		precision: precision,
		scale:     scale,
	})
}

type typeDecimal128 struct {
	precision uint8
	scale     uint8
}

func (t typeDecimal128) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

func (t typeDecimal128) Binary() []byte {
	return []byte{BinaryTypeDecimal128[0], t.precision, t.scale}
}

func (t typeDecimal128) Write(w Writer, value decimal.Decimal) error {
	return NotImplementedError
}

func (t typeDecimal128) Scan(r Reader, v *decimal.Decimal) error {
	return NotImplementedError
}
