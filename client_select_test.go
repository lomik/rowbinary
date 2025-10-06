package rowbinary

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func TestClient_Select(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	assert.NoError(c.Select(ctx,
		"SELECT * FROM system.numbers LIMIT 5",
		WithFormatReader(func(r *FormatReader) error {
			var numbers []uint64

			for r.Next() {
				var num uint64
				Scan(r, UInt64, &num)
				numbers = append(numbers, num)
			}
			assert.Equal([]uint64{0, 1, 2, 3, 4}, numbers)
			return r.Err()
		}),
	))

	assert.ErrorContains(c.Select(ctx,
		"SELECT * FROM system.numbers LIMIT 5",
		WithFormatReader(func(r *FormatReader) error {
			i := 0
			for r.Next() {
				i++
				if i > 1000 {
					return errors.New("infinite loop")
				}
				var num uint32
				Scan(r, UInt32, &num)
			}
			return r.Err()
		}),
	), "type mismatch")

	assert.ErrorContains(c.Select(ctx,
		"SELECT * FROM system.numbers LIMIT 5",
		RowBinary,
		WithFormatReader(func(r *FormatReader) error {
			for r.Next() {
				var num uint64
				Scan(r, UInt64, &num)
			}
			return r.Err()
		}),
	), "columns must be set")

	assert.NoError(c.Select(ctx,
		"SELECT * FROM system.numbers LIMIT 5",
		RowBinary,
		C("", UInt64),
		WithFormatReader(func(r *FormatReader) error {
			var numbers []uint64

			for r.Next() {
				var num uint64
				Scan(r, UInt64, &num)
				numbers = append(numbers, num)
			}
			assert.Equal([]uint64{0, 1, 2, 3, 4}, numbers)
			return r.Err()
		})))

	assert.ErrorContains(c.Select(ctx, "SELECT * FROM system.numbers LIMIT 5",
		RowBinary,
		C("", UInt32),
		WithFormatReader(func(r *FormatReader) error {
			for r.Next() {
				var num uint64
				Scan(r, UInt64, &num)
			}

			return r.Err()
		})), "type mismatch")
}

func TestClient_Select_ExternalData(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	assert.NoError(c.Select(ctx,
		"SELECT max(value) FROM data1",
		WithFormatReader(func(r *FormatReader) error {
			var numbers []uint64

			for r.Next() {
				var num uint64
				assert.NoError(Scan(r, UInt64, &num))
				numbers = append(numbers, num)
			}
			assert.Equal([]uint64{4}, numbers)
			return r.Err()
		}),
		WithExternalData("data1",
			func(w *FormatWriter) error {
				for i := range uint64(5) {
					if err := Write(w, UInt64, i); err != nil {
						return err
					}
				}
				return nil
			},
			C("value", UInt64),
		),
	))
}

func TestClient_Select_WithBodyReader(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	var body []byte
	assert.NoError(c.Select(ctx,
		"SELECT 42 AS value",
		WithBodyReader(func(r io.Reader) error {
			var err error
			body, err = io.ReadAll(r)
			return err
		}),
	))

	// Verify the body is not empty
	assert.NotEmpty(body)
}

func TestClient_Select_WithHeader(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	// Test with a custom header - ClickHouse may ignore unknown headers
	assert.NoError(c.Select(ctx,
		"SELECT 1 AS value",
		WithHeader("X-Test-Header", "test-value"),
		WithFormatReader(func(r *FormatReader) error {
			var numbers []uint8

			for r.Next() {
				var num uint8
				Scan(r, UInt8, &num)
				numbers = append(numbers, num)
			}
			assert.Equal([]uint8{1}, numbers)
			return r.Err()
		}),
	))
}

func TestClient_Select_CancelledContext(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	cancelCtx, cancel := context.WithCancel(ctx)
	cancel() // Cancel immediately

	err := c.Select(cancelCtx,
		"SELECT 1 AS value",
		WithFormatReader(func(r *FormatReader) error {
			return nil
		}),
	)
	assert.Error(err)
	assert.Contains(err.Error(), "context canceled")
}

func TestClient_Select_InvalidDSN(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	// Create client with invalid DSN
	c := NewClient(ctx, "http://invalid-host:8123")
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err := c.Select(ctx,
		"SELECT 1 AS value",
		WithFormatReader(func(r *FormatReader) error {
			return nil
		}),
	)
	assert.Error(err)
	// Should contain connection error
	// assert.Contains(err.Error(), "context deadline exceeded") // or similar connection error
}
