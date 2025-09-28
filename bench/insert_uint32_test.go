package bench

import (
	"context"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/lomik/rowbinary"
	"github.com/stretchr/testify/assert"
)

func BenchmarkRowbinary_Insert_UInt32(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x UInt32) ENGINE = Null"))

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
				for i := range uint32(100000) {
					if err := rowbinary.Write(r, rowbinary.UInt32, i); err != nil {
						return err
					}
				}
				return nil
			}, rowbinary.NewColumn("x", rowbinary.UInt32)),
		)
	}

	b.StopTimer()
}

func BenchmarkRowbinary_Insert_UInt32_Any(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x UInt32) ENGINE = Null"))

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Insert(ctx, "t", func(r *rowbinary.FormatWriter) error {
				for i := range uint32(100000) {
					if err := r.WriteAny(i); err != nil {
						return err
					}
				}
				return nil
			}, rowbinary.NewColumn("x", rowbinary.UInt32)),
		)
	}

	b.StopTimer()
}

func BenchmarkNative_Insert_UInt32(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Execute(ctx, "CREATE TABLE t (x UInt32) ENGINE = Null"))

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

		for i := range uint32(100000) {
			assert.NoError(batch.Append(i))
		}

		assert.NoError(batch.Send())
	}

	b.StopTimer()
}
