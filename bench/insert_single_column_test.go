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
	benchmarkSingleColumn(b, rowbinary.UInt32, uint32(42), 100000)
}

func benchmarkSingleColumn[T any](b *testing.B, tp rowbinary.Type[T], value T, count int) {
	b.Run(fmt.Sprintf("rowbinary_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		tc := newTestCase()
		defer tc.Close()

		ctx := context.Background()
		c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
			Database: tc.Database(),
		})

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
				}, rowbinary.NewColumn("x", tp)),
			)
		}

		b.StopTimer()

	})

	b.Run(fmt.Sprintf("rowbinary_any_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		tc := newTestCase()
		defer tc.Close()

		ctx := context.Background()
		c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
			Database: tc.Database(),
		})

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
				}, rowbinary.NewColumn("x", tp)),
			)
		}

		b.StopTimer()
	})

	b.Run(fmt.Sprintf("native_%s", tp.String()), func(b *testing.B) {
		assert := assert.New(b)
		tc := newTestCase()
		defer tc.Close()

		ctx := context.Background()
		c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
			Database: tc.Database(),
		})

		assert.NoError(c.Exec(ctx, fmt.Sprintf("CREATE TABLE t (x %s) ENGINE = Null", tp.String())))

		conn, err := clickhouse.Open(&clickhouse.Options{
			Addr: []string{testClickHouseNativeAddr},
			Auth: clickhouse.Auth{
				Database: tc.Database(),
			},
		})

		assert.NoError(err)
		defer conn.Close()

		b.ResetTimer()

		for b.Loop() {
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO t")
			assert.NoError(err)

			for range count {
				assert.NoError(batch.Append(value))
			}

			assert.NoError(batch.Send())
		}

		b.StopTimer()
	})
}
