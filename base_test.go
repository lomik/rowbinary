package rowbinary

import (
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
	BenchmarkType(b, Date, ValueDate{Year: 2023, Month: 11, Day: 22})
	BenchmarkType(b, TupleAny(UInt32, String), []any{uint32(42), "hello world"})
	BenchmarkType(b, LowCardinality(String), "hello world")
	BenchmarkType(b, LowCardinalityAny(String), "hello world")
	BenchmarkType(b, Bool, false)
	BenchmarkType(b, Bool, true)
	BenchmarkType(b, FixedString(10), []byte("hello\x00\x00\x00\x00\x00"))
	BenchmarkType(b, TupleNamedAny(C("i", UInt32), C("s", String)), []any{uint32(42), "hello world"})
	BenchmarkType(b, Date32, ValueDate{2250, 3, 5})
}
