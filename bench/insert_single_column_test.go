package bench

import (
	"context"
	"fmt"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/lomik/rowbinary"
	"github.com/stretchr/testify/assert"
)

func BenchmarkSingleColumn(b *testing.B) {
	b.SetParallelism(1)
	arrayUInt32 := make([]uint32, 1000)
	for i := range arrayUInt32 {
		arrayUInt32[i] = uint32(i)
	}

	benchmarkSingleColumn(b, rowbinary.Array(rowbinary.UInt32), arrayUInt32, 100000)
	benchmarkSingleColumn(b, rowbinary.String, "hello world", 100000)
	benchmarkSingleColumn(b, rowbinary.UInt8, uint8(42), 100000)
	benchmarkSingleColumn(b, rowbinary.UInt16, uint16(42), 100000)
	benchmarkSingleColumn(b, rowbinary.UInt32, uint32(42), 100000)
	benchmarkSingleColumn(b, rowbinary.UInt64, uint64(42), 100000)
	benchmarkSingleColumn(b, rowbinary.Int8, int8(42), 100000)
	benchmarkSingleColumn(b, rowbinary.Int16, int16(42), 100000)
	benchmarkSingleColumn(b, rowbinary.Int32, int32(42), 100000)
	benchmarkSingleColumn(b, rowbinary.Int64, int64(42), 100000)
	benchmarkSingleColumn(b, rowbinary.Float32, float32(123.123), 100000)
	benchmarkSingleColumn(b, rowbinary.Float64, float64(123.123), 100000)

	// @TODO more types
}

func benchmarkSingleColumn[T any](b *testing.B, tp rowbinary.Type[T], value T, count int) {
	b.Run(fmt.Sprintf("rowbinary_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		ctx := context.Background()
		c := rowbinary.NewTestClient(ctx, testClickHouseDSN)
		defer c.Close()

		assert.NoError(c.Exec(ctx, fmt.Sprintf("CREATE TABLE t (x %s) ENGINE = Null", tp.String())))

		b.ResetTimer()

		for b.Loop() {
			assert.NoError(
				c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
					for range count {
						if err := rowbinary.Write(r, tp, value); err != nil {
							return err
						}
					}
					return nil
				}, rowbinary.C("x", tp)),
			)
		}

		b.StopTimer()

	})

	b.Run(fmt.Sprintf("rowbinary_any_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		ctx := context.Background()
		c := rowbinary.NewTestClient(ctx, testClickHouseDSN)
		defer c.Close()

		assert.NoError(c.Exec(ctx, fmt.Sprintf("CREATE TABLE t (x %s) ENGINE = Null", tp.String())))

		b.ResetTimer()

		for b.Loop() {
			assert.NoError(
				c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
					for range count {
						if err := r.WriteAny(value); err != nil {
							return err
						}
					}
					return nil
				}, rowbinary.C("x", tp)),
			)
		}

		b.StopTimer()
	})

	b.Run(fmt.Sprintf("native_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		ctx := context.Background()
		c := rowbinary.NewTestClient(ctx, testClickHouseDSN)
		defer c.Close()

		assert.NoError(c.Exec(ctx, fmt.Sprintf("CREATE TABLE t (x %s) ENGINE = Null", tp.String())))

		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{testClickHouseNativeAddr},
			Auth: clickhouse.Auth{
				Database: c.Database(),
			},
		})

		assert.NoError(err)
		defer conn.Close()

		b.ResetTimer()

		for b.Loop() {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
			assert.NoError(err)

			for range count {
				batch.Append(value)
			}

			assert.NoError(batch.Send())
		}

		b.StopTimer()
	})
}
