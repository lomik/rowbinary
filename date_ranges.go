package rowbinary

import (
	"fmt"
	"math"
	"time"
)

// Date32 range supported by ClickHouse.
var (
	date32Min = ValueDate{Year: 1900, Month: 1, Day: 1}
	date32Max = ValueDate{Year: 2299, Month: 12, Day: 31}
)

// validDateTime returns value as a UTC time, or an error if it is not a real
// calendar date (time.Date would silently normalize 2024-13-45 to 2025-02-14).
func validDateTime(value ValueDate) (time.Time, error) {
	v := time.Date(int(value.Year), time.Month(value.Month), int(value.Day), 0, 0, 0, 0, time.UTC)
	if v.Year() != int(value.Year) || v.Month() != time.Month(value.Month) || v.Day() != int(value.Day) {
		return time.Time{}, fmt.Errorf("invalid calendar date: %04d-%02d-%02d", value.Year, value.Month, value.Day)
	}
	return v, nil
}

func (d ValueDate) before(o ValueDate) bool {
	if d.Year != o.Year {
		return d.Year < o.Year
	}
	if d.Month != o.Month {
		return d.Month < o.Month
	}
	return d.Day < o.Day
}

// dateTimeSeconds converts value to DateTime's uint32 seconds, rejecting
// instants before 1970-01-01 00:00:00 UTC or after 2106-02-07 06:28:15 UTC.
func dateTimeSeconds(value time.Time) (uint32, error) {
	s := value.Unix()
	if s < 0 || s > math.MaxUint32 {
		return 0, fmt.Errorf("DateTime out of range (1970-01-01 00:00:00 to 2106-02-07 06:28:15 UTC): %s", value.UTC().Format(time.RFC3339))
	}
	return uint32(s), nil
}

// dateTime64Ticks converts value to DateTime64 ticks at the given precision,
// rejecting values that do not fit in int64.
func dateTime64Ticks(value time.Time, precision int64) (int64, error) {
	scale := intPow(10, precision)
	sec := value.Unix()
	frac := int64(value.Nanosecond()) / intPow(10, 9-precision)
	errRange := fmt.Errorf("DateTime64(%d) cannot represent %s", precision, value.UTC().Format(time.RFC3339Nano))
	if sec > (math.MaxInt64-frac)/scale {
		return 0, errRange
	}
	if sec < math.MinInt64/scale {
		// sec*scale alone underflows, but frac >= 0 can bring the sum back in
		// range when sec is exactly one below the quotient.
		if sec < math.MinInt64/scale-1 {
			return 0, errRange
		}
		hi := (sec+1)*scale + frac
		if hi < math.MinInt64+scale {
			return 0, errRange
		}
		return hi - scale, nil
	}
	return sec*scale + frac, nil
}
