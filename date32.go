package rowbinary

import (
	"time"
)

var Date32 Type[time.Time] = MakeTypeWrapAny[time.Time](typeDate32{})

type typeDate32 struct{}

var days1900 = int32(25567) // to 1970
var seconds1900 = int64(days1900 * secInDay)

func (t typeDate32) String() string {
	return "Date32"
}

func (t typeDate32) Binary() []byte {
	return BinaryTypeDate32[:]
}

func (t typeDate32) Write(w Writer, value time.Time) error {
	v := time.Date(value.Year(), value.Month(), value.Day(), 0, 0, 0, 0, time.UTC)
	days := int32((v.Unix()) / secInDay)
	return Int32.Write(w, days)
}

func (t typeDate32) Read(r Reader) (time.Time, error) {
	n, err := Int32.Read(r)
	if err != nil {
		return time.Time{}, err
	}
	v := time.Unix(int64(n)*secInDay, 0).UTC()
	return v, nil
}

func (t typeDate32) Scan(r Reader, v *time.Time) error {
	val, err := t.Read(r)
	if err != nil {
		return err
	}
	*v = val
	return nil
}
