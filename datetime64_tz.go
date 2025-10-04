package rowbinary

import (
	"fmt"
	"slices"
	"time"
)

var _ Type[time.Time] = DateTime64TZ(6, "Asia/Istanbul")

func DateTime64TZ(p uint8, tz string) Type[time.Time] {
	loc, locErr := time.LoadLocation(tz)
	return MakeTypeWrapAny[time.Time](typeDateTime64TZ{precision: int64(p), tz: tz, loc: loc, locErr: locErr})
}

type typeDateTime64TZ struct {
	precision int64
	tz        string
	loc       *time.Location
	locErr    error
}

func (t typeDateTime64TZ) String() string {
	return fmt.Sprintf("DateTime64(%d, %s)", t.precision, quote(t.tz))
}

func (t typeDateTime64TZ) Binary() []byte {
	return slices.Concat(BinaryTypeDateTime64WithTimeZone[:], []byte{uint8(t.precision)}, UVarintEncode(uint64(len(t.tz))), []byte(t.tz))
}

func (t typeDateTime64TZ) Write(w Writer, value time.Time) error {
	if t.locErr != nil {
		return t.locErr
	}
	return Int64.Write(w, value.UnixNano()/intPow(10, 9-t.precision))
}

func (t typeDateTime64TZ) Scan(r Reader, v *time.Time) error {
	if t.locErr != nil {
		return t.locErr
	}
	var n int64
	err := Int64.Scan(r, &n)
	if err != nil {
		return err
	}
	*v = time.Unix(0, n*intPow(10, 9-t.precision)).In(t.loc)
	return nil
}
