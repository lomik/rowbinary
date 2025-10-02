package rowbinary

import (
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
		return UInt16.Write(w, 0)
	}

	v := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	days := uint16((v.Unix()) / secInDay)
	return UInt16.Write(w, days)
}

func (t typeDate) Read(r Reader) (time.Time, error) {
	n, err := UInt16.Read(r)
	if err != nil {
		return time.Time{}, err
	}
	v := time.Unix(int64(n)*secInDay, 0).UTC()
	return v, nil
}

func (t typeDate) Scan(r Reader, v *time.Time) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
