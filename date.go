package rowbinary

import (
	"fmt"
	"time"
)

// secInDay represents seconds in day.
//
// NB: works only on UTC, use time.Date, time.Time.AddDate.
const secInDay = 24 * 60 * 60

var Date Type[time.Time] = MakeTypeWrapAny[time.Time](typeDate{})

type typeDate struct{}

func (t typeDate) String() string {
	return "Date"
}

func (t typeDate) Binary() []byte {
	return BinaryTypeDate[:]
}

func (t typeDate) Write(w Writer, value time.Time) error {
	if value.Year() < 1970 {
		return fmt.Errorf("invalid date: %s", value)
	}

	v := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	days := uint16(v.Unix() / secInDay)
	return UInt16.Write(w, days)
}

func (t typeDate) Scan(r Reader, v *time.Time) error {
	var n uint16
	err := UInt16.Scan(r, &n)
	if err != nil {
		return err
	}
	*v = time.Unix(int64(n)*secInDay, 0).UTC()
	return nil
}
