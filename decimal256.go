package rowbinary

import (
	"fmt"

	"github.com/shopspring/decimal"
)

func Decimal256(precision uint8, scale uint8) Type[decimal.Decimal] {
	return MakeTypeWrapAny(typeDecimal256{
		precision: precision,
		scale:     scale,
	})
}

type typeDecimal256 struct {
	precision uint8
	scale     uint8
}

func (t typeDecimal256) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

func (t typeDecimal256) Binary() []byte {
	return []byte{BinaryTypeDecimal256[0], t.precision, t.scale}
}

func (t typeDecimal256) Write(w Writer, value decimal.Decimal) error {
	return NotImplementedError
}

func (t typeDecimal256) Scan(r Reader, v *decimal.Decimal) error {
	return NotImplementedError
}
