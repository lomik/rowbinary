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
	return Int64.Write(w, value.UnixNano()/intPow(10, 9-t.precision))
}

func intPow(base, exponent int64) int64 {
	result := int64(1)
	for i := int64(0); i < exponent; i++ {
		result *= base
	}
	return result
}

func (t typeDateTime64) Scan(r Reader, v *time.Time) error {
	var n int64
	err := Int64.Scan(r, &n)
	if err != nil {
		return err
	}
	*v = time.Unix(0, n*intPow(10, 9-t.precision)).UTC()
	return nil
}
