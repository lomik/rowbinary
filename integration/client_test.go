package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/lomik/rowbinary"
)

func TestClient_Exec(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := integrationClient(t)

	err := c.Exec(ctx, "CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1")
	assert.NoError(err)

	err = c.Exec(ctx, "CREATE TABLE")
	assert.ErrorContains(err, "Syntax error")
}

func TestClient_Select(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := integrationClient(t)

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
	c := integrationClient(t)

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
			C("value", UInt64),
			WithFormatWriter(func(w *FormatWriter) error {
				for i := range uint64(5) {
					if err := Write(w, UInt64, i); err != nil {
						return err
					}
				}
				return nil
			}),
		),
	))
}

func TestClient_Select_WithBodyReader(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := integrationClient(t)

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
	c := integrationClient(t)

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
	c := integrationClient(t)

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

func TestClient_Insert(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := integrationClient(t)

	assert.NoError(c.Exec(ctx, "CREATE TABLE t1 (x String) ENGINE = Memory"))

	assert.NoError(c.Insert(ctx,
		"t1",
		C("x", String),
		WithFormatWriter(func(r *FormatWriter) error {
			for i := range 5 {
				if err := Write(r, String, fmt.Sprintf("%d", i)); err != nil {
					return err
				}
			}
			return nil
		}),
	))

	assert.ErrorContains(c.Insert(ctx,
		"t1",
		C("x", String),
		WithFormatWriter(func(r *FormatWriter) error {
			for i := range 5 {
				if err := Write(r, String, fmt.Sprintf("%d", i)); err != nil {
					return err
				}
			}
			return fmt.Errorf("insertion failed")
		}),
	), "insertion failed")

}

// A query that fails after ClickHouse has started streaming rows still
// returns HTTP 200; the failure must surface as an error from Select.
func TestClient_Select_MidStreamFailure(t *testing.T) {
	c := integrationClient(t)
	const query = "SELECT number FROM system.numbers WHERE throwIf(number = 300000) = 0 LIMIT 400000 SETTINGS max_block_size = 1000"

	err := c.Select(t.Context(), query, WithFormatReader(func(r *FormatReader) error {
		for r.Next() {
			var n uint64
			if Scan(r, UInt64, &n) != nil {
				break
			}
		}
		return nil // deliberately ignores r.Err()
	}))
	if err == nil {
		t.Fatal("Select with reader: mid-stream failure reported as success")
	}

	err = c.Select(t.Context(), query)
	if err == nil {
		t.Fatal("Select without reader: mid-stream failure reported as success")
	}
	if !strings.Contains(err.Error(), "FUNCTION_THROW_IF_VALUE_IS_NON_ZERO") {
		t.Fatalf("error does not carry the ClickHouse exception: %v", err)
	}

	// A successful query is unaffected.
	if err := c.Select(t.Context(), "SELECT number FROM system.numbers LIMIT 400000 SETTINGS max_block_size = 1000"); err != nil {
		t.Fatal(err)
	}
}

// Dynamic(max_types=0) must encode exactly as the server does: compare with the
// raw type bytes in a RowBinaryWithNamesAndTypes header, then read a value.
func TestClient_Select_DynamicMaxTypesZero(t *testing.T) {
	c := integrationClient(t)
	const query = "SELECT CAST([42, 43]::Array(Int64), 'Dynamic(max_types=0)') AS value"
	tp := Dynamic(0, Array(Int64))

	err := c.Select(t.Context(), query, WithUseBinaryHeader(true), WithBodyReader(func(r io.Reader) error {
		body, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		// Header: 1 column, name "value", then the binary type.
		prefix := append([]byte{1, 5}, "value"...)
		if !bytes.HasPrefix(body, prefix) || !bytes.HasPrefix(body[len(prefix):], tp.Binary()) {
			return fmt.Errorf("server header % x does not contain Dynamic(0) as % x", body[:min(len(body), 16)], tp.Binary())
		}
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, binaryHeader := range []bool{false, true} {
		err := c.Select(t.Context(), query, WithUseBinaryHeader(binaryHeader), C("value", tp),
			WithFormatReader(func(r *FormatReader) error {
				if !r.Next() {
					return fmt.Errorf("no rows: %w", r.Err())
				}
				var v Value
				if err := Scan(r, tp, &v); err != nil {
					return err
				}
				if got, ok := v.Value.([]int64); !ok || len(got) != 2 || got[0] != 42 || got[1] != 43 {
					return fmt.Errorf("value = %#v", v)
				}
				return nil
			}))
		if err != nil {
			t.Errorf("binary header %v: %v", binaryHeader, err)
		}
	}
}
