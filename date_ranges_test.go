package rowbinary

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"testing"
	"time"
)

func TestDateTimeWriteRange(t *testing.T) {
	for _, tp := range []Type[time.Time]{DateTime, DateTimeTZ("UTC")} {
		for _, bad := range []time.Time{
			time.Date(1960, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Unix(-1, 0),
			time.Unix(math.MaxUint32+1, 0),
			time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC),
		} {
			if err := tp.Write(NewWriter(io.Discard), bad); err == nil {
				t.Errorf("%s accepted %s", tp, bad.UTC())
			}
		}
		for _, ok := range []time.Time{time.Unix(0, 0), time.Unix(math.MaxUint32, 0)} {
			var buf bytes.Buffer
			if err := tp.Write(NewWriter(&buf), ok); err != nil {
				t.Errorf("%s rejected %s: %v", tp, ok.UTC(), err)
				continue
			}
			var back time.Time
			if err := tp.Scan(NewReader(&buf), &back); err != nil || !back.Equal(ok) {
				t.Errorf("%s round-trip %s -> %s (%v)", tp, ok.UTC(), back.UTC(), err)
			}
		}
	}
}

func TestDateTime64WriteOverflow(t *testing.T) {
	for _, tp := range []Type[time.Time]{DateTime64(9), DateTime64TZ(9, "UTC")} {
		for _, bad := range []time.Time{
			time.Date(2299, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(1677, 1, 1, 0, 0, 0, 0, time.UTC),
		} {
			if err := tp.Write(NewWriter(io.Discard), bad); err == nil {
				t.Errorf("%s accepted %s", tp, bad)
			}
		}
	}
	// The int64 limits at precision 9 are exactly representable.
	for _, ok := range []time.Time{time.Unix(0, math.MaxInt64), time.Unix(0, math.MinInt64)} {
		var buf bytes.Buffer
		if err := DateTime64(9).Write(NewWriter(&buf), ok); err != nil {
			t.Errorf("DateTime64(9) rejected %s: %v", ok.UTC(), err)
			continue
		}
		var back time.Time
		if err := DateTime64(9).Scan(NewReader(&buf), &back); err != nil || !back.Equal(ok) {
			t.Errorf("DateTime64(9) round-trip %s -> %s (%v)", ok.UTC(), back.UTC(), err)
		}
	}
	// Precision 0 still covers far dates.
	if err := DateTime64(0).Write(NewWriter(io.Discard), time.Date(2299, 12, 31, 23, 59, 59, 0, time.UTC)); err != nil {
		t.Errorf("DateTime64(0) rejected 2299-12-31: %v", err)
	}
}

func TestDate32WriteRangeAndCalendar(t *testing.T) {
	for _, bad := range []ValueDate{
		{1800, 1, 1},
		{1899, 12, 31},
		{2300, 1, 1},
		{3000, 1, 1},
		{2024, 13, 45},
		{2023, 2, 29},
	} {
		if err := Date32.Write(NewWriter(io.Discard), bad); err == nil {
			t.Errorf("Date32 accepted %v", bad)
		}
	}
	for _, ok := range []ValueDate{{1900, 1, 1}, {2299, 12, 31}, {2024, 2, 29}} {
		var buf bytes.Buffer
		if err := Date32.Write(NewWriter(&buf), ok); err != nil {
			t.Errorf("Date32 rejected %v: %v", ok, err)
			continue
		}
		var back ValueDate
		if err := Date32.Scan(NewReader(&buf), &back); err != nil || back != ok {
			t.Errorf("Date32 round-trip %v -> %v (%v)", ok, back, err)
		}
	}
}

func TestDate32ScanUnrepresentableYear(t *testing.T) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], math.MaxInt32)
	var v ValueDate
	if err := Date32.Scan(NewReader(bytes.NewReader(b[:])), &v); err == nil {
		t.Fatalf("MaxInt32 days scanned as %+v", v)
	}
}

func TestDateRejectsInvalidCalendarDate(t *testing.T) {
	for _, bad := range []ValueDate{{2024, 13, 45}, {2023, 2, 29}, {2023, 4, 31}} {
		if err := Date.Write(NewWriter(io.Discard), bad); err == nil {
			t.Errorf("Date accepted %v", bad)
		}
	}
}
