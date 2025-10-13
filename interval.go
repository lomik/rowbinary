package rowbinary

import "encoding/binary"

// https://clickhouse.com/docs/sql-reference/data-types/special-data-types/interval
// https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding#interval-kind-binary-encoding
type intervalKind uint8

const intervalKindNanosecond intervalKind = 0x00
const intervalKindMicrosecond intervalKind = 0x01
const intervalKindMillisecond intervalKind = 0x02
const intervalKindSecond intervalKind = 0x03
const intervalKindMinute intervalKind = 0x04
const intervalKindHour intervalKind = 0x05
const intervalKindDay intervalKind = 0x06
const intervalKindWeek intervalKind = 0x07
const intervalKindMonth intervalKind = 0x08
const intervalKindQuarter intervalKind = 0x09
const intervalKindYear intervalKind = 0x1A

var IntervalNanosecond Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindNanosecond})
var IntervalMicrosecond Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindMicrosecond})
var IntervalMillisecond Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindMillisecond})
var IntervalSecond Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindSecond})
var IntervalMinute Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindMinute})
var IntervalHour Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindHour})
var IntervalDay Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindDay})
var IntervalWeek Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindWeek})
var IntervalMonth Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindMonth})
var IntervalQuarter Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindQuarter})
var IntervalYear Type[int64] = MakeTypeWrapAny[int64](typeInterval{kind: intervalKindYear})

type typeInterval struct {
	kind intervalKind
}

func (t typeInterval) String() string {
	switch t.kind {
	case intervalKindNanosecond:
		return "IntervalNanosecond"
	case intervalKindMicrosecond:
		return "IntervalMicrosecond"
	case intervalKindMillisecond:
		return "IntervalMillisecond"
	case intervalKindSecond:
		return "IntervalSecond"
	case intervalKindMinute:
		return "IntervalMinute"
	case intervalKindHour:
		return "IntervalHour"
	case intervalKindDay:
		return "IntervalDay"
	case intervalKindWeek:
		return "IntervalWeek"
	case intervalKindMonth:
		return "IntervalMonth"
	case intervalKindQuarter:
		return "IntervalQuarter"
	case intervalKindYear:
		return "IntervalYear"
	}
	return "Interval"
}

func (t typeInterval) Binary() []byte {
	return append(BinaryTypeInterval[:], byte(t.kind))
}

func (t typeInterval) Write(w Writer, value int64) error {
	return UInt64.Write(w, uint64(value))
}

func (t typeInterval) Scan(r Reader, v *int64) (err error) {
	b, err := r.Peek(8)
	if err != nil {
		return err
	}
	*v = int64(binary.LittleEndian.Uint64(b))
	if _, err = r.Discard(8); err != nil {
		return err
	}
	return
}
