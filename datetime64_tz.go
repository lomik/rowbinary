package rowbinary

import (
	"fmt"
	"slices"
	"time"
)

var _ Type[time.Time] = DateTime64TZ(6, "Asia/Istanbul")

func DateTime64TZ(p uint8, tz string) Type[time.Time] {
	loc, locErr := loadLocation(tz)
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
	return slices.Concat(BinaryTypeDateTime64WithTimeZone[:], []byte{uint8(t.precision)}, VarintEncode(uint64(len(t.tz))), []byte(t.tz))
}

func (t typeDateTime64TZ) Write(w Writer, value time.Time) error {
	if err := checkDateTime64Precision(t.precision); err != nil {
		return err
	}
	if t.locErr != nil {
		return t.locErr
	}
	ticks, err := dateTime64Ticks(value, t.precision)
	if err != nil {
		return err
	}
	return Int64.Write(w, ticks)
}

func (t typeDateTime64TZ) Scan(r Reader, v *time.Time) error {
	if err := checkDateTime64Precision(t.precision); err != nil {
		return err
	}
	if t.locErr != nil {
		return t.locErr
	}
	var n int64
	err := Int64.Scan(r, &n)
	if err != nil {
		return err
	}
	scale := intPow(10, t.precision)
	*v = time.Unix(n/scale, (n%scale)*intPow(10, 9-t.precision)).In(t.loc)
	return nil
}
