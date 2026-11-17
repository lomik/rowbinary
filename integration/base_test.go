package integration

import (
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	. "github.com/lomik/rowbinary"
)

func pointer[V any](v V) *V {
	return &v
}

func null[V any](_ V) *V {
	return nil
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestBase(t *testing.T) {
	TestTypeWithDSN(t, Nullable(Nothing), null(any(nil)), "SELECT NULL", testDSN)
	TestTypeWithDSN(t, String, "hello world", "SELECT toString('hello world')", testDSN)
	TestTypeWithDSN(t, StringBytes, []byte("hello world"), "SELECT toString('hello world')", testDSN)
	TestTypeWithDSN(t, String, "", "SELECT toString('')", testDSN)
	TestTypeWithDSN(t, UInt8, uint8(42), "SELECT toUInt8(42)", testDSN)
	TestTypeWithDSN(t, UInt16, uint16(42), "SELECT toUInt16(42)", testDSN)
	TestTypeWithDSN(t, UInt32, uint32(42), "SELECT toUInt32(42)", testDSN)
	TestTypeWithDSN(t, UInt64, uint64(42), "SELECT toUInt64(42)", testDSN)
	TestTypeWithDSN(t, Int8, int8(42), "SELECT toInt8(42)", testDSN)
	TestTypeWithDSN(t, Int16, int16(42), "SELECT toInt16(42)", testDSN)
	TestTypeWithDSN(t, Int32, int32(42), "SELECT toInt32(42)", testDSN)
	TestTypeWithDSN(t, Int64, int64(42), "SELECT toInt64(42)", testDSN)
	TestTypeWithDSN(t, Int8, int8(-42), "SELECT toInt8(-42)", testDSN)
	TestTypeWithDSN(t, Int16, int16(-42), "SELECT toInt16(-42)", testDSN)
	TestTypeWithDSN(t, Int32, int32(-42), "SELECT toInt32(-42)", testDSN)
	TestTypeWithDSN(t, Int64, int64(-42), "SELECT toInt64(-42)", testDSN)
	TestTypeWithDSN(t, Float64, float64(123.123), "SELECT toFloat64(123.123)", testDSN)
	TestTypeWithDSN(t, Float32, float32(123.123), "SELECT toFloat32(123.123)", testDSN)
	TestTypeWithDSN(t, IPv4, netip.MustParseAddr("127.0.0.1").As4(), "SELECT toIPv4('127.0.0.1')", testDSN)
	TestTypeWithDSN(t, IPv6, netip.MustParseAddr("2001:db8::68").As16(), "SELECT toIPv6('2001:db8::68')", testDSN)
	TestTypeWithDSN(t, Array(UInt32), []uint32{3123213123, 42, 0}, "SELECT [toUInt32(3123213123), toUInt32(42), toUInt32(0)]", testDSN)
	TestTypeWithDSN(t, Array(String), []string{"hello world", "string2", ""}, "SELECT ['hello world', 'string2', '']", testDSN)
	TestTypeWithDSN(t, Array(Int64), []int64{123123123213123, -2, 0}, "SELECT [toInt64(123123123213123), toInt64(-2), toInt64(0)]", testDSN)
	TestTypeWithDSN(t, ArrayAny(UInt32), []any{uint32(3123213123), uint32(42), uint32(0)}, "SELECT [toUInt32(3123213123), toUInt32(42), toUInt32(0)]", testDSN)
	TestTypeWithDSN(t, ArrayAny(String), []any{"hello world", "string2", ""}, "SELECT ['hello world', 'string2', '']", testDSN)
	TestTypeWithDSN(t, ArrayAny(Int64), []any{int64(123123123213123), int64(-2), int64(0)}, "SELECT [toInt64(123123123213123), toInt64(-2), toInt64(0)]", testDSN)
	TestTypeWithDSN(t, UUID, uuid.MustParse("258b07b7-daa1-4c80-8062-58a2e07c2601"), "SELECT toUUID('258b07b7-daa1-4c80-8062-58a2e07c2601')", testDSN)
	TestTypeWithDSN(t, Decimal(9, 4), decimal.New(42000, -4), "SELECT toDecimal32(4.2, 4)", testDSN)
	TestTypeWithDSN(t, Decimal(9, 4), decimal.New(-42000, -4), "SELECT toDecimal32(-4.2, 4)", testDSN)
	TestTypeWithDSN(t, Decimal(18, 4), decimal.New(42000, -4), "SELECT toDecimal64(4.2, 4)", testDSN)
	TestTypeWithDSN(t, Decimal(18, 4), decimal.New(-42000, -4), "SELECT toDecimal64(-4.2, 4)", testDSN)
	TestTypeWithDSN(t, Map(String, String), map[string]string{"key": "value"}, "SELECT map('key', 'value')", testDSN)
	TestTypeWithDSN(t, MapKV(String, String), NewKV[string, string]().Append("key", "value"), "SELECT map('key', 'value')", testDSN)
	TestTypeWithDSN(t, Map(UInt32, Map(String, String)), map[uint32]map[string]string{42: {"key": "value"}}, "SELECT map(toUInt32(42), map('key', 'value'))", testDSN)
	TestTypeWithDSN(t, MapAny(String, String), map[any]any{"key": "value"}, "SELECT map('key', 'value')", testDSN)
	TestTypeWithDSN(t, Nullable(Int32), pointer(int32(-42)), "SELECT toNullable(toInt32(-42))", testDSN)
	TestTypeWithDSN(t, Nullable(Int32), null(int32(-42)), "SELECT nullIf(toInt32(-42), toInt32(-42))", testDSN)
	TestTypeWithDSN(t, NullableAny(Int32), pointer(any(int32(-42))), "SELECT toNullable(toInt32(-42))", testDSN)
	TestTypeWithDSN(t, NullableAny(Int32), nil, "SELECT nullIf(toInt32(-42), toInt32(-42))", testDSN)
	TestTypeWithDSN(t, DateTime, time.Date(2023, 11, 22, 20, 49, 31, 0, time.UTC), "SELECT toDateTime('2023-11-22 20:49:31')", testDSN)
	TestTypeWithDSN(t, Date, ValueDate{Year: 2023, Month: 11, Day: 22}, "SELECT toDate('2023-11-22')", testDSN)
	TestTypeWithDSN(t, Date, ValueDate{Year: 2023, Month: 3, Day: 5}, "SELECT toDate('2023-03-05')", testDSN)
	TestTypeWithDSN(t, TupleAny(UInt32, String), []any{uint32(42), "hello world"}, "SELECT tuple(toUInt32(42), 'hello world')", testDSN)
	TestTypeWithDSN(t, LowCardinality(String), "hello world", "CREATE TEMPORARY TABLE tmp (value LowCardinality(String)) ENGINE=Memory; INSERT INTO tmp (value) VALUES ('hello world'); SELECT value FROM tmp", testDSN)
	TestTypeWithDSN(t, LowCardinalityAny(String), "hello world", "CREATE TEMPORARY TABLE tmp (value LowCardinality(String)) ENGINE=Memory; INSERT INTO tmp (value) VALUES ('hello world'); SELECT value FROM tmp", testDSN)
	TestTypeWithDSN(t, Bool, false, "SELECT false", testDSN)
	TestTypeWithDSN(t, Bool, true, "SELECT true", testDSN)
	TestTypeWithDSN(t, FixedString(10), []byte("hello\x00\x00\x00\x00\x00"), "SELECT toFixedString('hello', 10)", testDSN)
	TestTypeWithDSN(t, TupleNamedAny(C("i", UInt32), C("s", String)), []any{uint32(42), "hello world"}, "CREATE TEMPORARY TABLE tmp (`value` Tuple(i UInt32, s String)) ENGINE = Memory; INSERT INTO tmp VALUES ((42, 'hello world')); SELECT value FROM tmp", testDSN)
	// TestType(t, Date32, time.Date(1899, 12, 10, 0, 0, 0, 0, time.UTC), "SELECT toDate32('1899-12-10')")
	TestTypeWithDSN(t, Date32, ValueDate{Year: 1900, Month: 1, Day: 1}, "SELECT toDate32('1900-01-01')", testDSN)
	TestTypeWithDSN(t, Date32, ValueDate{Year: 1970, Month: 1, Day: 1}, "SELECT toDate32('1970-01-01')", testDSN)
	TestTypeWithDSN(t, Date32, ValueDate{Year: 2025, Month: 7, Day: 5}, "SELECT toDate32('2025-07-05')", testDSN)
	TestTypeWithDSN(t, Date32, ValueDate{Year: 2250, Month: 3, Day: 5}, "SELECT toDate32('2250-03-05')", testDSN)
	TestTypeWithDSN(t, DateTimeTZ("Asia/Shanghai"), time.Date(2025, 3, 11, 23, 43, 2, 0, must(time.LoadLocation("Asia/Shanghai"))), "SELECT toDateTime('2025-03-11 23:43:02', 'Asia/Shanghai')", testDSN)
	TestTypeWithDSN(t, Array(TupleNamedAny(C("i", UInt32), C("s", String))),
		[][]any{
			{uint32(1), "sss"},
			{uint32(42), "hello world"},
		}, `
		CREATE TEMPORARY TABLE tmp (
			value Nested (
				i UInt32,
				s String
			)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ([1,42], ['sss','hello world']);
		SELECT value FROM tmp
		`, testDSN)
	TestTypeWithDSN(t, Enum8(map[string]int8{"android": 1, "ios": 2, "windows": -10}), "ios", `
		CREATE TEMPORARY TABLE tmp (
			value Enum('android'=1, 'ios'=2, 'windows'=-10)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ('ios');
		SELECT value FROM tmp
		`, testDSN)
	TestTypeWithDSN(t, Enum16(map[string]int16{"android": 1024, "ios": 2248, "windows": -3000}), "ios", `
		CREATE TEMPORARY TABLE tmp (
			value Enum('android'=1024, 'ios'=2248, 'windows'=-3000)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ('ios');
		SELECT value FROM tmp
		`, testDSN)
	TestTypeWithDSN(t, DateTime64(9),
		time.Date(2023, 11, 22, 20, 49, 31, 123456789, time.UTC),
		"SELECT toDateTime64('2023-11-22 20:49:31.123456789', 9)", testDSN)

	TestTypeWithDSN(t, DateTime64(3),
		time.Date(2023, 11, 22, 20, 49, 31, 123000000, time.UTC),
		"SELECT makeDateTime64(2023,11,22,20,49,31,123, 3)", testDSN)

	TestTypeWithDSN(t, DateTime64TZ(9, "Asia/Shanghai"),
		time.Date(2023, 11, 22, 20, 49, 31, 123456789, must(time.LoadLocation("Asia/Shanghai"))),
		"SELECT toDateTime64('2023-11-22 20:49:31.123456789', 9, 'Asia/Shanghai')", testDSN)

	TestTypeWithDSN(t, DateTime64TZ(3, "Asia/Shanghai"),
		time.Date(2023, 11, 22, 20, 49, 31, 123000000, must(time.LoadLocation("Asia/Shanghai"))),
		"SELECT makeDateTime64(2023,11,22,20,49,31,123, 3, 'Asia/Shanghai')", testDSN)

	TestTypeWithDSN(t, Point, []any{float64(123.45), float64(543.21)}, `
		CREATE TEMPORARY TABLE tmp (
			value Point
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ((123.45, 543.21));
		SELECT value FROM tmp
		`, testDSN)
	TestTypeWithDSN(t, Variant(Array(UInt32), String, UInt32), Value{Type: String, Value: "ios"}, `
		CREATE TEMPORARY TABLE tmp (
			value Variant(UInt32, String, Array(UInt32))
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ('ios');
		SELECT value FROM tmp
		`, testDSN)
	TestTypeWithDSN(t, Variant(Array(UInt32), String, UInt32), Value{Type: Array(UInt32), Value: []uint32{42, 43}}, `
		CREATE TEMPORARY TABLE tmp (
			value Variant(UInt32, String, Array(UInt32))
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ([42,43]);
		SELECT value FROM tmp
		`, testDSN)

	TestTypeWithDSN(t, Dynamic(DynamicDefaultMaxTypes, Array(Int64)), Value{Type: Array(Int64), Value: []int64{42, 43}}, `
		CREATE TEMPORARY TABLE tmp (
			value Dynamic()
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ([42,43]);
		SELECT value FROM tmp
		`, testDSN)

	TestTypeWithDSN(t, Dynamic(42, Array(Int64)), Value{Type: Array(Int64), Value: []int64{42, 43}}, `
		CREATE TEMPORARY TABLE tmp (
			value Dynamic(max_types=42)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ([42,43]);
		SELECT value FROM tmp
		`, testDSN)

	TestTypeWithDSN(t, IntervalDay, 42, "SELECT INTERVAL 42 DAY", testDSN)
	TestTypeWithDSN(t, IntervalWeek, -42, "SELECT INTERVAL -42 WEEK", testDSN)
}
