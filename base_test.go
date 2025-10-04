package rowbinary

import (
	"net/netip"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func pointer[V any](v V) *V {
	return &v
}

func null[V any](_ V) *V {
	return nil
}

func TestBase(t *testing.T) {
	TestType(t, Nullable(Nothing), null(any(nil)), "SELECT NULL")
	TestType(t, String, "hello world", "SELECT toString('hello world')")
	TestType(t, StringBytes, []byte("hello world"), "SELECT toString('hello world')")
	TestType(t, String, "", "SELECT toString('')")
	TestType(t, UInt8, uint8(42), "SELECT toUInt8(42)")
	TestType(t, UInt16, uint16(42), "SELECT toUInt16(42)")
	TestType(t, UInt32, uint32(42), "SELECT toUInt32(42)")
	TestType(t, UInt64, uint64(42), "SELECT toUInt64(42)")
	TestType(t, Int8, int8(42), "SELECT toInt8(42)")
	TestType(t, Int16, int16(42), "SELECT toInt16(42)")
	TestType(t, Int32, int32(42), "SELECT toInt32(42)")
	TestType(t, Int64, int64(42), "SELECT toInt64(42)")
	TestType(t, Int8, int8(-42), "SELECT toInt8(-42)")
	TestType(t, Int16, int16(-42), "SELECT toInt16(-42)")
	TestType(t, Int32, int32(-42), "SELECT toInt32(-42)")
	TestType(t, Int64, int64(-42), "SELECT toInt64(-42)")
	TestType(t, Float64, float64(123.123), "SELECT toFloat64(123.123)")
	TestType(t, Float32, float32(123.123), "SELECT toFloat32(123.123)")
	TestType(t, IPv4, netip.MustParseAddr("127.0.0.1").As4(), "SELECT toIPv4('127.0.0.1')")
	TestType(t, IPv6, netip.MustParseAddr("2001:db8::68").As16(), "SELECT toIPv6('2001:db8::68')")
	TestType(t, Array(UInt32), []uint32{3123213123, 42, 0}, "SELECT [toUInt32(3123213123), toUInt32(42), toUInt32(0)]")
	TestType(t, Array(String), []string{"hello world", "string2", ""}, "SELECT ['hello world', 'string2', '']")
	TestType(t, Array(Int64), []int64{123123123213123, -2, 0}, "SELECT [toInt64(123123123213123), toInt64(-2), toInt64(0)]")
	TestType(t, ArrayAny(UInt32), []any{uint32(3123213123), uint32(42), uint32(0)}, "SELECT [toUInt32(3123213123), toUInt32(42), toUInt32(0)]")
	TestType(t, ArrayAny(String), []any{"hello world", "string2", ""}, "SELECT ['hello world', 'string2', '']")
	TestType(t, ArrayAny(Int64), []any{int64(123123123213123), int64(-2), int64(0)}, "SELECT [toInt64(123123123213123), toInt64(-2), toInt64(0)]")
	TestType(t, UUID, uuid.MustParse("258b07b7-daa1-4c80-8062-58a2e07c2601"), "SELECT toUUID('258b07b7-daa1-4c80-8062-58a2e07c2601')")
	TestType(t, Decimal(9, 4), decimal.New(42000, -4), "SELECT toDecimal32(4.2, 4)")
	TestType(t, Decimal(9, 4), decimal.New(-42000, -4), "SELECT toDecimal32(-4.2, 4)")
	TestType(t, Decimal(18, 4), decimal.New(42000, -4), "SELECT toDecimal64(4.2, 4)")
	TestType(t, Decimal(18, 4), decimal.New(-42000, -4), "SELECT toDecimal64(-4.2, 4)")
	TestType(t, Map(String, String), map[string]string{"key": "value"}, "SELECT map('key', 'value')")
	TestType(t, MapKV(String, String), NewKV[string, string]().Set("key", "value"), "SELECT map('key', 'value')")
	TestType(t, Map(UInt32, Map(String, String)), map[uint32]map[string]string{42: {"key": "value"}}, "SELECT map(toUInt32(42), map('key', 'value'))")
	TestType(t, MapAny(String, String), map[any]any{"key": "value"}, "SELECT map('key', 'value')")
	TestType(t, Nullable(Int32), pointer(int32(-42)), "SELECT toNullable(toInt32(-42))")
	TestType(t, Nullable(Int32), null(int32(-42)), "SELECT nullIf(toInt32(-42), toInt32(-42))")
	TestType(t, NullableAny(Int32), pointer(any(int32(-42))), "SELECT toNullable(toInt32(-42))")
	TestType(t, NullableAny(Int32), nil, "SELECT nullIf(toInt32(-42), toInt32(-42))")
	TestType(t, DateTime, time.Date(2023, 11, 22, 20, 49, 31, 0, time.UTC), "SELECT toDateTime('2023-11-22 20:49:31')")
	TestType(t, Date, time.Date(2023, 11, 22, 0, 0, 0, 0, time.UTC), "SELECT toDate('2023-11-22')")
	TestType(t, Date, time.Date(2023, 3, 5, 0, 0, 0, 0, time.UTC), "SELECT toDate('2023-03-05')")
	TestType(t, TupleAny(UInt32, String), []any{uint32(42), "hello world"}, "SELECT tuple(toUInt32(42), 'hello world')")
	TestType(t, LowCardinality(String), "hello world", "CREATE TEMPORARY TABLE tmp (value LowCardinality(String)) ENGINE=Memory; INSERT INTO tmp (value) VALUES ('hello world'); SELECT value FROM tmp")
	TestType(t, LowCardinalityAny(String), "hello world", "CREATE TEMPORARY TABLE tmp (value LowCardinality(String)) ENGINE=Memory; INSERT INTO tmp (value) VALUES ('hello world'); SELECT value FROM tmp")
	TestType(t, Bool, false, "SELECT false")
	TestType(t, Bool, true, "SELECT true")
	TestType(t, FixedString(10), []byte("hello\x00\x00\x00\x00\x00"), "SELECT toFixedString('hello', 10)")
	TestType(t, TupleNamedAny(C("i", UInt32), C("s", String)), []any{uint32(42), "hello world"}, "CREATE TEMPORARY TABLE tmp (`value` Tuple(i UInt32, s String)) ENGINE = Memory; INSERT INTO tmp VALUES ((42, 'hello world')); SELECT value FROM tmp")
	// TestType(t, Date32, time.Date(1899, 12, 10, 0, 0, 0, 0, time.UTC), "SELECT toDate32('1899-12-10')")
	TestType(t, Date32, time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC), "SELECT toDate32('1900-01-01')")
	TestType(t, Date32, time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC), "SELECT toDate32('1970-01-01')")
	TestType(t, Date32, time.Date(2025, 7, 5, 0, 0, 0, 0, time.UTC), "SELECT toDate32('2025-07-05')")
	TestType(t, Date32, time.Date(2250, 3, 5, 0, 0, 0, 0, time.UTC), "SELECT toDate32('2250-03-05')")
	TestType(t, DateTimeTZ("Asia/Shanghai"), time.Date(2025, 3, 11, 23, 43, 2, 0, must(time.LoadLocation("Asia/Shanghai"))), "SELECT toDateTime('2025-03-11 23:43:02', 'Asia/Shanghai')")
	TestType(t, Array(TupleNamedAny(C("i", UInt32), C("s", String))),
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
		`)
	TestType(t, Enum8(map[string]int8{"android": 1, "ios": 2, "windows": -10}), "ios", `
		CREATE TEMPORARY TABLE tmp (
			value Enum('android'=1, 'ios'=2, 'windows'=-10)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ('ios');
		SELECT value FROM tmp
		`)
	TestType(t, Enum16(map[string]int16{"android": 1024, "ios": 2248, "windows": -3000}), "ios", `
		CREATE TEMPORARY TABLE tmp (
			value Enum('android'=1024, 'ios'=2248, 'windows'=-3000)
		) ENGINE = Memory;
		INSERT INTO tmp VALUES ('ios');
		SELECT value FROM tmp
		`)
	TestType(t, DateTime64(9),
		time.Date(2023, 11, 22, 20, 49, 31, 123456789, time.UTC),
		"SELECT toDateTime64('2023-11-22 20:49:31.123456789', 9)")

	TestType(t, DateTime64(3),
		time.Date(2023, 11, 22, 20, 49, 31, 123000000, time.UTC),
		"SELECT makeDateTime64(2023,11,22,20,49,31,123, 3)")

	TestType(t, DateTime64TZ(9, "Asia/Shanghai"),
		time.Date(2023, 11, 22, 20, 49, 31, 123456789, must(time.LoadLocation("Asia/Shanghai"))),
		"SELECT toDateTime64('2023-11-22 20:49:31.123456789', 9, 'Asia/Shanghai')")

	TestType(t, DateTime64TZ(3, "Asia/Shanghai"),
		time.Date(2023, 11, 22, 20, 49, 31, 123000000, must(time.LoadLocation("Asia/Shanghai"))),
		"SELECT makeDateTime64(2023,11,22,20,49,31,123, 3, 'Asia/Shanghai')")
}

func BenchmarkBase(b *testing.B) {
	BenchmarkType(b, Nullable(Nothing), null(any(nil)))
	BenchmarkType(b, String, "hello world")
	BenchmarkType(b, String, "")
	BenchmarkType(b, UInt8, uint8(42))
	BenchmarkType(b, UInt16, uint16(42))
	BenchmarkType(b, UInt32, uint32(42))
	BenchmarkType(b, UInt64, uint64(42))
	BenchmarkType(b, Int8, int8(42))
	BenchmarkType(b, Int16, int16(42))
	BenchmarkType(b, Int32, int32(42))
	BenchmarkType(b, Int64, int64(42))
	BenchmarkType(b, Int8, int8(-42))
	BenchmarkType(b, Int16, int16(-42))
	BenchmarkType(b, Int32, int32(-42))
	BenchmarkType(b, Int64, int64(-42))
	BenchmarkType(b, Float64, float64(123.123))
	BenchmarkType(b, Float32, float32(123.123))
	BenchmarkType(b, Array(UInt32), []uint32{3123213123, 42, 0})
	BenchmarkType(b, Array(String), []string{"hello world", "string2", ""})
	BenchmarkType(b, Array(Int64), []int64{123123123213123, -2, 0})
	BenchmarkType(b, ArrayAny(UInt32), []any{uint32(3123213123), uint32(42), uint32(0)})
	BenchmarkType(b, ArrayAny(String), []any{"hello world", "string2", ""})
	BenchmarkType(b, ArrayAny(Int64), []any{int64(123123123213123), int64(-2), int64(0)})
	BenchmarkType(b, UUID, uuid.MustParse("258b07b7-daa1-4c80-8062-58a2e07c2601"))
	BenchmarkType(b, Decimal(9, 4), decimal.New(42000, -4))
	BenchmarkType(b, Decimal(9, 4), decimal.New(-42000, -4))
	BenchmarkType(b, Decimal(18, 4), decimal.New(42000, -4))
	BenchmarkType(b, Decimal(18, 4), decimal.New(-42000, -4))
	BenchmarkType(b, Map(String, String), map[string]string{"key": "value"})
	BenchmarkType(b, Map(Int64, Int64), map[int64]int64{42: 15})
	BenchmarkType(b, MapKV(Int64, Int64), NewKV[int64, int64]().Append(42, 15))
	BenchmarkType(b, MapAny(String, String), map[any]any{"key": "value"})
	BenchmarkType(b, Nullable(Int32), pointer(int32(-42)))
	BenchmarkType(b, Nullable(Int32), null(int32(-42)))
	BenchmarkType(b, NullableAny(Int32), pointer(any(int32(-42))))
	BenchmarkType(b, NullableAny(Int32), nil)
	BenchmarkType(b, DateTime, time.Date(2023, 11, 22, 20, 49, 31, 0, time.UTC))
	BenchmarkType(b, Date, time.Date(2023, 11, 22, 0, 0, 0, 0, time.UTC))
	BenchmarkType(b, Date, time.Date(2023, 3, 5, 0, 0, 0, 0, time.UTC))
	BenchmarkType(b, Date, time.Date(2023, 3, 5, 0, 0, 0, 0, time.UTC))
	BenchmarkType(b, TupleAny(UInt32, String), []any{uint32(42), "hello world"})
	BenchmarkType(b, LowCardinality(String), "hello world")
	BenchmarkType(b, LowCardinalityAny(String), "hello world")
	BenchmarkType(b, Bool, false)
	BenchmarkType(b, Bool, true)
	BenchmarkType(b, FixedString(10), []byte("hello\x00\x00\x00\x00\x00"))
	BenchmarkType(b, TupleNamedAny(C("i", UInt32), C("s", String)), []any{uint32(42), "hello world"})
}
