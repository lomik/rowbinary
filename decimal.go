package rowbinary

import (
	"fmt"

	"github.com/shopspring/decimal"
)

func Decimal(precision uint8, scale uint8) Type[decimal.Decimal] {
	return MakeTypeWrapAny(typeDecimal{
		precision: precision,
		scale:     scale,
	})
}

type typeDecimal struct {
	precision uint8
	scale     uint8
}

func (t typeDecimal) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

func (t typeDecimal) Binary() []byte {
	if t.precision <= 9 {
		// decimal32
		return []byte{BinaryTypeDecimal32[0], t.precision, t.scale}
	} else {
		return []byte{BinaryTypeDecimal64[0], t.precision, t.scale}
	}
}

func (t typeDecimal) Write(w Writer, value decimal.Decimal) error {
	// decimal32
	if t.precision <= 9 {
		part := uint32(decimal.NewFromBigInt(value.Coefficient(), value.Exponent()+int32(t.scale)).IntPart())
		return UInt32.Write(w, part)
	}

	// decimal64
	if t.precision <= 18 {
		part := uint64(decimal.NewFromBigInt(value.Coefficient(), value.Exponent()+int32(t.scale)).IntPart())
		return UInt64.Write(w, part)
	}

	// todo: decimal128, decimal256
	return ErrNotImplemented
}

func (t typeDecimal) Read(r Reader) (decimal.Decimal, error) {
	// decimal32
	if t.precision <= 9 {
		v, err := Int32.Read(r)
		if err != nil {
			return decimal.Zero, err
		}
		return decimal.New(int64(v), -int32(t.scale)), nil
	}

	// decimal64
	if t.precision <= 18 {
		v, err := Int64.Read(r)
		if err != nil {
			return decimal.Zero, err
		}
		return decimal.New(int64(v), -int32(t.scale)), nil
	}

	// todo: decimal128, decimal256

	return decimal.Zero, ErrNotImplemented
}

func (t typeDecimal) Scan(r Reader, v *decimal.Decimal) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
