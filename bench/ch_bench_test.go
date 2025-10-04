package bench

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/lomik/rowbinary"

	"github.com/stretchr/testify/assert"
)

// https://github.com/ClickHouse/ch-bench

func BenchmarkChBench_Rowbinary(b *testing.B) {
	assert := assert.New(b)

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, nil)

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Select(ctx, "SELECT * FROM system.numbers_mt LIMIT 500000000", func(r *rowbinary.FormatReader) error {
				max := uint64(0)
				var v uint64
				for r.Next() {
					err := rowbinary.Scan(r, rowbinary.UInt64, &v)
					if err != nil {
						return err
					}
					if v > max {
						max = v
					}
				}
				assert.Equal(uint64(499999999), max)
				return nil
			}),
		)
	}

	b.StopTimer()
}

func BenchmarkChBench_ClickhouseGo(b *testing.B) {
	assert := assert.New(b)

	ctx := context.Background()
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{testClickHouseNativeAddr},
	})

	assert.NoError(err)
	defer conn.Close()

	b.ResetTimer()

	for b.Loop() {
		func() {
			var x uint64
			var max uint64

			rows, err := conn.Query(ctx, "SELECT * FROM system.numbers_mt LIMIT 500000000")
			assert.NoError(err)

			for rows.Next() {
				err = rows.Scan(&x)
				if err != nil {
					break
				}
				if x > max {
					max = x
				}
			}
			assert.Equal(uint64(499999999), max)
			assert.NoError(rows.Err())
			rows.Close()
		}()
	}

	b.StopTimer()
}
