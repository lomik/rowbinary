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

// Enum labels with separators, brackets and escapes must survive the text
// header ClickHouse sends (RowBinaryWithNamesAndTypes without binary types).
func TestClient_Select_EnumTrickyLabels(t *testing.T) {
	c := integrationClient(t)
	labels := map[string]int8{"a, b": 1, "c (d)": 2, "e f": 3, "it's": 4, `x\y`: 5, "nl\n": 6, "q=1": 7}
	tp := Enum8(labels)
	for want := range labels {
		query := fmt.Sprintf("SELECT CAST(%s, '%s') AS v", quoteSQL(want), strings.ReplaceAll(strings.ReplaceAll(tp.String(), `\`, `\\`), `'`, `\'`))
		err := c.Select(t.Context(), query, WithUseBinaryHeader(false), C("v", tp),
			WithFormatReader(func(r *FormatReader) error {
				if !r.Next() {
					return fmt.Errorf("no rows: %w", r.Err())
				}
				var got string
				if err := Scan(r, tp, &got); err != nil {
					return err
				}
				if got != want {
					return fmt.Errorf("got %q", got)
				}
				return nil
			}))
		if err != nil {
			t.Errorf("label %q: %v", want, err)
		}
	}
}

func quoteSQL(s string) string {
	r := strings.NewReplacer(`\`, `\\`, `'`, `\'`, "\n", `\n`)
	return "'" + r.Replace(s) + "'"
}

// RowBinary has no header, so ClickHouse maps values to table columns by
// position; a column list in the table argument selects and orders them.
func TestClient_Insert_RowBinaryColumnOrder(t *testing.T) {
	c := integrationClient(t)
	ctx := t.Context()
	if err := c.Exec(ctx, "CREATE TABLE ord (a UInt32, b UInt32, c String DEFAULT 'dflt') ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	// Rows are written as (b, a): b=2, a=1.
	insert := func(table string) error {
		return c.Insert(ctx, table, RowBinary, C("b", UInt32), C("a", UInt32),
			WithFormatWriter(func(w *FormatWriter) error {
				if err := Write(w, UInt32, 2); err != nil {
					return err
				}
				return Write(w, UInt32, 1)
			}))
	}
	read := func() (rows [][3]any) {
		t.Helper()
		err := c.Select(ctx, "SELECT a, b, c FROM ord", WithFormatReader(func(r *FormatReader) error {
			for r.Next() {
				var a, b uint32
				var s string
				if err := r.Scan(&a, &b, &s); err != nil {
					return err
				}
				rows = append(rows, [3]any{a, b, s})
			}
			return r.Err()
		}))
		if err != nil {
			t.Fatal(err)
		}
		return rows
	}

	// Documented route: the column list in the table argument.
	if err := insert("ord (b, a)"); err != nil {
		t.Fatal(err)
	}
	if got := read(); len(got) != 1 || got[0] != [3]any{uint32(1), uint32(2), "dflt"} {
		t.Fatalf("with column list: rows %v, want [1 2 dflt]", got)
	}

	// Without a list, RowBinary values go to (a, b, c) by position: the third
	// column is missing from the data, so ClickHouse rejects the insert.
	if err := insert("ord"); err == nil {
		t.Fatal("RowBinary without a column list for a 3-column table succeeded")
	}
}

func TestClient_Insert_QuoteIdentifier(t *testing.T) {
	c := integrationClient(t)
	ctx := t.Context()
	name := "we`ird. na\\me; x"
	table := QuoteIdentifier(name)
	if err := c.Exec(ctx, "CREATE TABLE "+table+" (v UInt8) ENGINE = Memory"); err != nil {
		t.Fatal(err)
	}
	err := c.Insert(ctx, table, C("v", UInt8), WithFormatWriter(func(w *FormatWriter) error {
		return Write(w, UInt8, 7)
	}))
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	err = c.Select(ctx, "SELECT name FROM system.tables WHERE database = currentDatabase()", WithFormatReader(func(r *FormatReader) error {
		for r.Next() {
			var s string
			if err := Scan(r, String, &s); err != nil {
				return err
			}
			got = append(got, s)
		}
		return r.Err()
	}))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != name {
		t.Fatalf("tables %q, want exactly %q", got, name)
	}
	var n uint64
	err = c.Select(ctx, "SELECT count() FROM "+table+" WHERE v = 7", WithFormatReader(func(r *FormatReader) error {
		if !r.Next() {
			return r.Err()
		}
		return Scan(r, UInt64, &n)
	}))
	if err != nil || n != 1 {
		t.Fatalf("count = %d, err %v", n, err)
	}
}

// NULLs read into `any` (r.Scan with types from the header) must be plain nil,
// at the top level and inside tuples and arrays.
func TestClient_Select_NullIntoAny(t *testing.T) {
	c := integrationClient(t)
	const query = "SELECT NULL::Nullable(String) AS a, (NULL, 1)::Tuple(Nullable(String), UInt8) AS b, [NULL, 5]::Array(Nullable(UInt8)) AS c, 'x'::Nullable(String) AS d"
	for _, binaryHeader := range []bool{false, true} {
		err := c.Select(t.Context(), query, WithUseBinaryHeader(binaryHeader), WithFormatReader(func(r *FormatReader) error {
			if !r.Next() {
				return fmt.Errorf("no rows: %w", r.Err())
			}
			var a, b, cc, d any
			if err := r.Scan(&a, &b, &cc, &d); err != nil {
				return err
			}
			if a != nil {
				return fmt.Errorf("a = %#v, want nil", a)
			}
			if tuple, ok := b.([]any); !ok || len(tuple) != 2 || tuple[0] != nil {
				return fmt.Errorf("b = %#v, want [nil 1]", b)
			}
			if arr, ok := cc.([]any); !ok || len(arr) != 2 || arr[0] != nil || arr[1] == nil {
				return fmt.Errorf("c = %#v, want [nil 5]", cc)
			}
			if p, ok := d.(*any); !ok || *p != "x" {
				return fmt.Errorf("d = %#v, want pointer to \"x\"", d)
			}
			return nil
		}))
		if err != nil {
			t.Errorf("binary header %v: %v", binaryHeader, err)
		}
	}
}
