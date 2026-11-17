package rowbinary

import (
	"fmt"
	"math"
	"time"
)

var Date32 Type[ValueDate] = MakeTypeWrapAny[ValueDate](typeDate32{})

type typeDate32 struct{}

func (t typeDate32) String() string {
	return "Date32"
}

func (t typeDate32) Binary() []byte {
	return BinaryTypeDate32[:]
}

func (t typeDate32) Write(w Writer, value ValueDate) error {
	if value.before(date32Min) || date32Max.before(value) {
		return fmt.Errorf("Date32 out of range (1900-01-01 to 2299-12-31): %04d-%02d-%02d", value.Year, value.Month, value.Day)
	}
	v, err := validDateTime(value)
	if err != nil {
		return err
	}
	days := int32(v.Unix() / secInDay)
	return Int32.Write(w, days)
}

func (t typeDate32) Scan(r Reader, v *ValueDate) error {
	var n int32
	err := Int32.Scan(r, &n)
	if err != nil {
		return err
	}
	tm := time.Unix(int64(n)*secInDay, 0).UTC()
	if tm.Year() < 0 || tm.Year() > math.MaxUint16 {
		return fmt.Errorf("Date32 value %d days is outside the representable range", n)
	}
	v.Year = uint16(tm.Year())
	v.Month = uint8(tm.Month())
	v.Day = uint8(tm.Day())
	return nil
}
