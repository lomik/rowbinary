package bench

import (
	"context"
	"testing"

	"github.com/lomik/rowbinary"
	"github.com/stretchr/testify/assert"
)

func BenchmarkRowbinary_Select_SystemNumbers(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Select(ctx, "SELECT * FROM system.numbers LIMIT 1000000", func(r *rowbinary.FormatReader) error {
				for r.Next() {
					if _, err := rowbinary.Read(r, rowbinary.UInt64); err != nil {
						return err
					}
				}
				return r.Err()
			}),
		)
	}

	b.StopTimer()
}

func BenchmarkRowbinary_Select_SystemNumbers_Any(b *testing.B) {
	assert := assert.New(b)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := rowbinary.NewClient(ctx, testClickHouseDSN, &rowbinary.ClientOptions{
		Database: tc.Database(),
	})

	b.ResetTimer()

	for b.Loop() {
		assert.NoError(
			c.Select(ctx, "SELECT * FROM system.numbers LIMIT 1000000", func(r *rowbinary.FormatReader) error {
				for r.Next() {
					if _, err := r.ReadAny(); err != nil {
						return err
					}
				}
				return r.Err()
			}),
		)
	}

	b.StopTimer()
}
