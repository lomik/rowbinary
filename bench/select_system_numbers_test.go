package bench

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/lomik/rowbinary"
	"github.com/stretchr/testify/assert"
)

func BenchmarkRowbinary_Select_SystemNumbers(b *testing.B) {
	assert := assert.New(b)

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, nil)

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Select(ctx, "SELECT * FROM system.numbers LIMIT 1000000", func(r *rowbinary.FormatReader) error {
				cnt := 0
				for r.Next() {
					if _, err := rowbinary.Read(r, rowbinary.UInt64); err != nil {
						return err
					}
					cnt++
				}
				assert.Equal(1000000, cnt)
				return r.Err()
			}),
		)
	}

	b.StopTimer()
}

func BenchmarkRowbinary_Select_SystemNumbers_Any(b *testing.B) {
	assert := assert.New(b)

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, nil)

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Select(ctx, "SELECT * FROM system.numbers LIMIT 1000000", func(r *rowbinary.FormatReader) error {
				cnt := 0
				var x any
				for r.Next() {
					if err := r.Scan(&x); err != nil {
						return err
					}
					cnt++
				}
				assert.Equal(1000000, cnt)
				return r.Err()
			}),
		)
	}

	b.StopTimer()
}

func BenchmarkNative_Select_SystemNumbers(b *testing.B) {
	assert := assert.New(b)

	ctx := context.Background()

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{testClickHouseNativeAddr},
	})

	assert.NoError(err)
	defer conn.Close()

	b.ResetTimer()

	for b.Loop() {
		var x uint64
		rows, err := conn.Query(ctx, "SELECT * FROM system.numbers LIMIT 1000000")
		assert.NoError(err)

		for rows.Next() {
			rows.Scan(&x)
		}
		rows.Close()

		assert.NoError(rows.Err())

	}

	b.StopTimer()
}
