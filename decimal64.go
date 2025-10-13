package rowbinary

import (
	"encoding/binary"
	"fmt"

	"github.com/shopspring/decimal"
)

func Decimal64(precision uint8, scale uint8) Type[decimal.Decimal] {
	return MakeTypeWrapAny(typeDecimal64{
		precision: precision,
		scale:     scale,
	})
}

type typeDecimal64 struct {
	precision uint8
	scale     uint8
}

func (t typeDecimal64) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

func (t typeDecimal64) Binary() []byte {
	return []byte{BinaryTypeDecimal64[0], t.precision, t.scale}
}

func (t typeDecimal64) Write(w Writer, value decimal.Decimal) error {
	part := uint64(decimal.NewFromBigInt(value.Coefficient(), value.Exponent()+int32(t.scale)).IntPart())
	binary.LittleEndian.PutUint64(w.Buffer(), part)
	_, err := w.Write(w.Buffer()[:8])
	return err
}

func (t typeDecimal64) Scan(r Reader, v *decimal.Decimal) error {
	b, err := r.Peek(8)
	if err != nil {
		return err
	}
	n := int64(binary.LittleEndian.Uint64(b))
	if _, err = r.Discard(8); err != nil {
		return err
	}
	*v = decimal.New(int64(n), -int32(t.scale))
	return nil
}
