package rowbinary

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func must[T any](v T, _ error) T {
	return v
}

func TestClient_Select(t *testing.T) {
	assert := assert.New(t)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := NewClient(ctx, testClickHouseDSN, &ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		var numbers []uint64

		for r.Next() {
			numbers = append(numbers, must(Read(r, UInt64)))
		}
		assert.Equal([]uint64{0, 1, 2, 3, 4}, numbers)
		return r.Err()
	}))

	assert.ErrorContains(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		i := 0
		for r.Next() {
			i++
			if i > 1000 {
				return errors.New("infinite loop")
			}
			Read(r, UInt32)
		}
		return r.Err()
	}), "type mismatch")

	assert.ErrorContains(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		for r.Next() {
			Read(r, UInt64)
		}
		return r.Err()
	}, RowBinary), "columns must be set")

	assert.NoError(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		var numbers []uint64

		for r.Next() {
			numbers = append(numbers, must(Read(r, UInt64)))
		}
		assert.Equal([]uint64{0, 1, 2, 3, 4}, numbers)
		return r.Err()
	}, RowBinary, NewColumn("", UInt64)))

	assert.ErrorContains(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		for r.Next() {
			Read(r, UInt64)
		}
		return r.Err()
	}, RowBinary, NewColumn("", UInt32)), "type mismatch")

	assert.ErrorContains(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5", func(r *FormatReader) error {
		for r.Next() {
			Read(r, UInt64)
		}
		return r.Err()
	}, UseBinaryHeader(false)), "not implemented")
}

func TestClient_Select_ExternalData(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	c := NewClient(ctx, testClickHouseDSN, nil)

	assert.NoError(c.Select(ctx, "SELECT max(value) FROM data1", func(r *FormatReader) error {
		var numbers []uint64

		for r.Next() {
			numbers = append(numbers, must(Read(r, UInt64)))
		}
		assert.Equal([]uint64{4}, numbers)
		return r.Err()
	}, ExternalData(
		"data1",
		func(w *FormatWriter) error {
			for i := uint64(0); i < 5; i++ {
				if err := Write(w, UInt64, i); err != nil {
					return err
				}
			}
			return nil
		},
		C("value", UInt64),
	)))
}
