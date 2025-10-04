package rowbinary

import (
	"fmt"
	"slices"
	"time"
)

var _ Type[time.Time] = DateTimeTZ("Asia/Istanbul")

func DateTimeTZ(tz string) Type[time.Time] {
	loc, locErr := time.LoadLocation(tz)
	return MakeTypeWrapAny[time.Time](typeDateTimeTZ{tz: tz, loc: loc, locErr: locErr})
}

type typeDateTimeTZ struct {
	tz     string
	loc    *time.Location
	locErr error
}

func (t typeDateTimeTZ) String() string {
	return fmt.Sprintf("DateTime(%s)", quote(t.tz))
}

func (t typeDateTimeTZ) Binary() []byte {
	return slices.Concat(BinaryTypeDateTimeWithTimeZone[:], UVarintEncode(uint64(len(t.tz))), []byte(t.tz))
}

func (t typeDateTimeTZ) Write(w Writer, value time.Time) error {
	if t.locErr != nil {
		return t.locErr
	}
	if value.Year() < 1970 {
		return UInt32.Write(w, 0)
	}
	return UInt32.Write(w, uint32(value.Unix()))
}

func (t typeDateTimeTZ) Read(r Reader) (time.Time, error) {
	if t.locErr != nil {
		return time.Time{}, t.locErr
	}

	n, err := UInt32.Read(r)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(n), 0).In(t.loc), nil
}

func (t typeDateTimeTZ) Scan(r Reader, v *time.Time) error {
	if t.locErr != nil {
		return t.locErr
	}
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
