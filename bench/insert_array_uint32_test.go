package bench

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/lomik/rowbinary"
	"github.com/stretchr/testify/assert"
)

var arrayUInt32 = rowbinary.Array(rowbinary.UInt32)

func BenchmarkRowbinary_Insert_ArrayUInt32(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x Array(UInt32)) ENGINE = Null"))

	data := make([]uint32, 1000)
	for i := range data {
		data[i] = uint32(i)
	}

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
				for range 100000 {
					if err := rowbinary.Write(r, arrayUInt32, data); err != nil {
						return err
					}
				}
				return nil
			}, rowbinary.NewColumn("x", arrayUInt32)),
		)
	}

	b.StopTimer()
}

func BenchmarkRowbinary_Insert_ArrayUInt32_Any(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x Array(UInt32)) ENGINE = Null"))

	data := make([]uint32, 1000)
	for i := range data {
		data[i] = uint32(i)
	}

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
				for range 100000 {
					if err := r.WriteAny(data); err != nil {
						return err
					}
				}
				return nil
			}, rowbinary.NewColumn("x", arrayUInt32)),
		)
	}

	b.StopTimer()
}

func BenchmarkNative_Insert_ArrayUInt32(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x Array(UInt32)) ENGINE = Null"))

	data := make([]uint32, 1000)
	for i := range data {
		data[i] = uint32(i)
	}

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

		for range 100000 {
			assert.NoError(batch.Append(data))
		}

		assert.NoError(batch.Send())
	}

	b.StopTimer()
}
