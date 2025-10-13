package rowbinary

import (
	"encoding/binary"
	"fmt"

	"github.com/shopspring/decimal"
)

func Decimal32(precision uint8, scale uint8) Type[decimal.Decimal] {
	return MakeTypeWrapAny(typeDecimal32{
		precision: precision,
		scale:     scale,
	})
}

type typeDecimal32 struct {
	precision uint8
	scale     uint8
}

func (t typeDecimal32) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

func (t typeDecimal32) Binary() []byte {
	return []byte{BinaryTypeDecimal32[0], t.precision, t.scale}

}

func (t typeDecimal32) Write(w Writer, value decimal.Decimal) error {
	part := uint32(decimal.NewFromBigInt(value.Coefficient(), value.Exponent()+int32(t.scale)).IntPart())
	binary.LittleEndian.PutUint32(w.Buffer(), part)
	_, err := w.Write(w.Buffer()[:4])
	return err
}

func (t typeDecimal32) Scan(r Reader, v *decimal.Decimal) error {
	b, err := r.Peek(4)
	if err != nil {
		return err
	}
	n := int32(binary.LittleEndian.Uint32(b))
	if _, err = r.Discard(4); err != nil {
		return err
	}
	*v = decimal.New(int64(n), -int32(t.scale))
	return nil
}
