package rowbinary

import (
	"time"
)

var DateTime Type[time.Time] = MakeTypeWrapAny[time.Time](typeDateTime{})

type typeDateTime struct{}

func (t typeDateTime) String() string {
	return "DateTime"
}

func (t typeDateTime) Binary() []byte {
	return BinaryTypeDateTime[:]
}

func (t typeDateTime) Write(w Writer, value time.Time) error {
	if value.Year() < 1970 {
		return UInt32.Write(w, 0)
	}
	return UInt32.Write(w, uint32(value.Unix()))
}

func (t typeDateTime) Scan(r Reader, v *time.Time) error {
	var n uint32
	err := UInt32.Scan(r, &n)
	if err != nil {
		return err
	}
	*v = time.Unix(int64(n), 0).UTC()
	return nil
}
