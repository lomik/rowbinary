package rowbinary

import (
	"fmt"
	"math/big"
	"slices"

	"github.com/shopspring/decimal"
)

// decimalBig encodes Decimal128 and Decimal256: the value times 10^scale as a
// little-endian two's-complement integer of size bytes, like Decimal32/64 but
// wider. Precision is at most 38 (Decimal128) or 76 (Decimal256), so any value
// within the precision also fits the storage.
type decimalBig struct {
	precision uint8
	scale     uint8
	size      int // 16 or 32 bytes
}

func (t decimalBig) String() string {
	return fmt.Sprintf("Decimal(%d, %d)", t.precision, t.scale)
}

// write truncates extra fractional digits, as Decimal32/64 do, and rejects
// values that do not fit the precision.
func (t decimalBig) write(w Writer, value decimal.Decimal) error {
	n := value.Shift(int32(t.scale)).Truncate(0).BigInt()
	limit := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(t.precision)), nil)
	if new(big.Int).Abs(n).Cmp(limit) >= 0 {
		return fmt.Errorf("value out of range for %s", t.String())
	}
	if n.Sign() < 0 {
		// Two's complement: 2^(8*size) + n.
		n.Add(n, new(big.Int).Lsh(big.NewInt(1), uint(8*t.size)))
	}
	buf := n.FillBytes(make([]byte, t.size)) // big-endian
	slices.Reverse(buf)
	_, err := w.Write(buf)
	return err
}

func (t decimalBig) scan(r Reader, v *decimal.Decimal) error {
	b, err := r.Peek(t.size)
	if err != nil {
		return err
	}
	buf := slices.Clone(b)
	if _, err = r.Discard(t.size); err != nil {
		return err
	}
	negative := buf[t.size-1]&0x80 != 0
	slices.Reverse(buf) // to big-endian
	n := new(big.Int).SetBytes(buf)
	if negative {
		n.Sub(n, new(big.Int).Lsh(big.NewInt(1), uint(8*t.size)))
	}
	// Same representation as Decimal32/64 when the value fits in int64, so
	// values compare equal with reflect.DeepEqual regardless of width.
	if n.IsInt64() {
		*v = decimal.New(n.Int64(), -int32(t.scale))
		return nil
	}
	*v = decimal.NewFromBigInt(n, -int32(t.scale))
	return nil
}
