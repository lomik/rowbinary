package rowbinary

import (
	"fmt"
	"time"
)

func DateTime64(p uint8) Type[time.Time] {
	return MakeTypeWrapAny[time.Time](typeDateTime64{
		precision: int64(p),
	})
}

type typeDateTime64 struct {
	precision int64
}

func (t typeDateTime64) String() string {
	return fmt.Sprintf("DateTime64(%d)", t.precision)
}

func (t typeDateTime64) Binary() []byte {
	return append(BinaryTypeDateTime64[:], uint8(t.precision))
}

func (t typeDateTime64) Write(w Writer, value time.Time) error {
	if err := checkDateTime64Precision(t.precision); err != nil {
		return err
	}
	ticks, err := dateTime64Ticks(value, t.precision)
	if err != nil {
		return err
	}
	return Int64.Write(w, ticks)
}

// maxDateTime64Precision is the largest precision ClickHouse allows for DateTime64.
const maxDateTime64Precision = 9

func checkDateTime64Precision(p int64) error {
	if p < 0 || p > maxDateTime64Precision {
		return fmt.Errorf("DateTime64 precision must be in range 0..%d, got %d", maxDateTime64Precision, p)
	}
	return nil
}

func intPow(base, exponent int64) int64 {
	result := int64(1)
	for i := int64(0); i < exponent; i++ {
		result *= base
	}
	return result
}

func (t typeDateTime64) Scan(r Reader, v *time.Time) error {
	if err := checkDateTime64Precision(t.precision); err != nil {
		return err
	}
	var n int64
	err := Int64.Scan(r, &n)
	if err != nil {
		return err
	}
	scale := intPow(10, t.precision)
	*v = time.Unix(n/scale, (n%scale)*intPow(10, 9-t.precision)).UTC()
	return nil
}
