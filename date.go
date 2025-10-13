package rowbinary

import (
	"fmt"
	"time"
)

// secInDay represents seconds in day.
const secInDay = 24 * 60 * 60

var Date Type[ValueDate] = MakeTypeWrapAny[ValueDate](typeDate{})

type typeDate struct{}

func (t typeDate) String() string {
	return "Date"
}

func (t typeDate) Binary() []byte {
	return BinaryTypeDate[:]
}

func (t typeDate) Write(w Writer, value ValueDate) error {
	if value.Year < 1970 {
		return fmt.Errorf("invalid date: %#v", value)
	}

	v := time.Date(int(value.Year), time.Month(value.Month), int(value.Day), 0, 0, 0, 0, time.UTC)
	days := uint16(v.Unix() / secInDay)
	return UInt16.Write(w, days)
}

func (t typeDate) Scan(r Reader, v *ValueDate) error {
	var n uint16
	err := UInt16.Scan(r, &n)
	if err != nil {
		return err
	}
	tm := time.Unix(int64(n)*secInDay, 0).UTC()
	v.Year = uint16(tm.Year())
	v.Month = uint8(tm.Month())
	v.Day = uint8(tm.Day())
	return nil
}
