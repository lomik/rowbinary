package rowbinary

import (
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
)

var _ Type[decimal.Decimal] = Decimal(18, 4)

type typeDecimal struct {
	id        uint64
	precision uint8
	scale     uint8
	tbin      []byte
	tstr      string
}

func Decimal(precision uint8, scale uint8) typeDecimal {
	var tbin []byte
	if precision <= 9 {
		// decimal32
		tbin = []byte{BinaryTypeDecimal32[0], precision, scale}
	} else {
		tbin = []byte{BinaryTypeDecimal64[0], precision, scale}
	}

	return typeDecimal{
		precision: precision,
		scale:     scale,
		tbin:      tbin,
		tstr:      fmt.Sprintf("Decimal(%d, %d)", precision, scale),
		id:        BinaryTypeID(tbin),
	}
}

func (t typeDecimal) String() string {
	return t.tstr
}

func (t typeDecimal) Binary() []byte {
	return t.tbin
}

func (t typeDecimal) ID() uint64 {
	return t.id
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

func (t typeDecimal) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}

func (t typeDecimal) WriteAny(w Writer, v any) error {
	value, ok := v.(decimal.Decimal)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}
