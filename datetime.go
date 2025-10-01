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

func (t typeDateTime) Read(r Reader) (time.Time, error) {
	n, err := UInt32.Read(r)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(int64(n), 0).UTC(), nil
}
