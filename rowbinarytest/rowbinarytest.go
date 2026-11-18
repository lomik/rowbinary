// Package rowbinarytest provides test helpers for code built on rowbinary:
// checking a Type against ClickHouse fixtures (TestType), benchmarking it
// (BenchmarkType), and running against a live server in an isolated database
// (TestClient). It is separate from rowbinary so that importing the library
// does not pull testing code into production builds.
package rowbinarytest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lomik/rowbinary"
)

// ExecLocal executes a ClickHouse query with `clickhouse local` and caches the
// output in fixtures/ch_<sha256 of query>.bin, relative to the working
// directory (the test's package directory under go test). A cached file is
// returned without running ClickHouse, so committed fixtures make tests run
// without a ClickHouse binary.
func ExecLocal(query string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(query))
	key := fmt.Sprintf("%x", h.Sum(nil))
	filename := fmt.Sprintf("fixtures/ch_%s.bin", key)

	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		body, err := exec.CommandContext(ctx, "clickhouse", "local", "--query", query).Output()
		if err != nil {
			return nil, err
		}

		err = os.WriteFile(filename, body, 0644)
		return body, err
	}
	// #nosec G304
	return os.ReadFile(filename)
}

var testClientCounter atomic.Uint64

// TestClient is a rowbinary.Client bound to a uniquely named database that
// Close drops.
type TestClient struct {
	rowbinary.Client
	db string
}

// NewTestClientE creates a uniquely named database on the server at dsn and
// returns a client that uses it, honoring the caller's context.
func NewTestClientE(ctx context.Context, dsn string, options ...rowbinary.ClientOption) (*TestClient, error) {
	db := fmt.Sprintf("db_%d_%d", testClientCounter.Add(1), time.Now().UnixNano())
	// Concat, not append: never write into the caller's options slice.
	c := rowbinary.NewClient(slices.Concat(options, []rowbinary.ClientOption{rowbinary.WithDSN(dsn), rowbinary.WithDatabase(db)})...)

	err := c.Exec(ctx, "CREATE DATABASE "+db, rowbinary.WithDatabase("default"))
	if err != nil {
		return nil, fmt.Errorf("create test database: %w", err)
	}

	return &TestClient{
		Client: c,
		db:     db,
	}, nil
}

// NewTestClientTB is NewTestClientE with a bounded setup, failing the test on
// error and dropping the database in t.Cleanup.
func NewTestClientTB(t testing.TB, dsn string, options ...rowbinary.ClientOption) *TestClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	c, err := NewTestClientE(ctx, dsn, options...)
	if err != nil {
		t.Fatalf("ClickHouse setup: %v", err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Errorf("ClickHouse database cleanup: %v", err)
		}
	})
	return c
}

// Close drops the test database.
func (tc *TestClient) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return tc.Exec(ctx, "DROP DATABASE "+tc.db)
}

// Database returns the name of the test database.
func (tc *TestClient) Database() string {
	return tc.db
}

// TestType checks tp against ClickHouse: query must produce one value (it is
// run as `<query> AS value FORMAT …` through ExecLocal), and value must be
// what tp writes and reads for it, in every RowBinary format.
func TestType[T any](t *testing.T, tp rowbinary.Type[T], value T, query string) {
	t.Helper()
	testTypeAt(t, 2, tp, value, query, "")
}

// TestTypeWithDSN runs the TestType checks and, if dsn is nonempty, also
// inserts value into a table on that server and reads it back.
func TestTypeWithDSN[T any](t *testing.T, tp rowbinary.Type[T], value T, query, dsn string) {
	t.Helper()
	testTypeAt(t, 2, tp, value, query, dsn)
}

// Fixture file names are the SHA-256 of the exact query text, so these
// suffixes must stay byte-for-byte identical (including the trailing spaces)
// or every committed RowBinaryWithNamesAndTypes fixture is orphaned. They are
// spelled with escapes so that editors cannot strip the whitespace.
const (
	namesAndTypesBinarySuffix = " AS value FORMAT RowBinaryWithNamesAndTypes \n\t\t\t\t\tSETTINGS \n\t\t\t\t\t\toutput_format_binary_encode_types_in_binary_format=1, \n\t\t\t\t\t\tsession_timezone='UTC'"
	namesAndTypesPlainSuffix  = " AS value FORMAT RowBinaryWithNamesAndTypes \n\t\t\t\t\tSETTINGS \n\t\t\t\t\t\toutput_format_binary_encode_types_in_binary_format=0, \n\t\t\t\t\t\tsession_timezone='UTC'"
)

type unknownType struct{}

// equal matches testify's assert.Equal: bytes.Equal for []byte, otherwise
// reflect.DeepEqual.
func equal(want, got any) bool {
	wb, ok1 := want.([]byte)
	gb, ok2 := got.([]byte)
	if ok1 && ok2 {
		return bytes.Equal(wb, gb)
	}
	return reflect.DeepEqual(want, got)
}

func checkEqual(t *testing.T, what string, want, got any) {
	t.Helper()
	if !equal(want, got) {
		t.Errorf("%s:\nwant %#v\n got %#v", what, want, got)
	}
}

func checkNoError(t *testing.T, what string, err error) {
	t.Helper()
	if err != nil {
		t.Errorf("%s: unexpected error: %v", what, err)
	}
}

func checkError(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil {
		t.Errorf("%s: expected an error, got nil", what)
	}
}

// testTypeAt labels subtests with the source location callerSkip frames above it,
// so wrappers can point the label at the test line instead of themselves.
func testTypeAt[T any](t *testing.T, callerSkip int, tp rowbinary.Type[T], value T, query, dsn string) {
	t.Helper()
	_, file, no, ok := runtime.Caller(callerSkip)
	if !ok {
		file = "unknown"
		no = 0
	}
	caller := fmt.Sprintf("(%s:%d)", filepath.Base(file), no)

	bodyEncoded, err := ExecLocal(query + " AS value FORMAT RowBinary SETTINGS session_timezone='UTC'")
	checkNoError(t, "ExecLocal", err)

	// compare Write with clickhouse
	t.Run(fmt.Sprintf("%s/write%s", tp.String(), caller), func(t *testing.T) {
		var buf bytes.Buffer
		checkNoError(t, "Write", tp.Write(rowbinary.NewWriter(&buf), value))
		checkEqual(t, "Write output", bodyEncoded, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/scan%s", tp.String(), caller), func(t *testing.T) {
		r := rowbinary.NewReader(bytes.NewBuffer(bodyEncoded))
		var v2 T
		checkNoError(t, "Scan", tp.Scan(r, &v2))
		checkEqual(t, "Scan result", value, v2)
	})

	// compare WriteAny with clickhouse
	t.Run(fmt.Sprintf("%s/write_any%s", tp.String(), caller), func(t *testing.T) {
		var buf bytes.Buffer
		checkNoError(t, "WriteAny", tp.WriteAny(rowbinary.NewWriter(&buf), value))
		checkEqual(t, "WriteAny output", bodyEncoded, buf.Bytes())
	})

	// write any wrong type: every candidate whose Go type is not T must fail
	t.Run(fmt.Sprintf("%s/write_any_wrong_type%s", tp.String(), caller), func(t *testing.T) {
		for _, v := range []any{"hello", 42} {
			if _, ok := v.(T); ok {
				continue
			}
			var buf bytes.Buffer
			checkError(t, fmt.Sprintf("WriteAny(%T)", v), tp.WriteAny(rowbinary.NewWriter(&buf), v))
		}
	})

	// write any of a type no codec accepts
	t.Run(fmt.Sprintf("%s/write_any_unknown_type%s", tp.String(), caller), func(t *testing.T) {
		var buf bytes.Buffer
		checkError(t, "WriteAny(unknownType)", tp.WriteAny(rowbinary.NewWriter(&buf), unknownType{}))
	})

	// every truncated prefix must fail to scan, including the one missing a single byte
	t.Run(fmt.Sprintf("%s/scan_truncated%s", tp.String(), caller), func(t *testing.T) {
		var buf bytes.Buffer
		checkNoError(t, "WriteAny", tp.WriteAny(rowbinary.NewWriter(&buf), value))

		var v2 T
		for i := 0; i < buf.Len(); i++ {
			r := rowbinary.NewReader(bytes.NewReader(buf.Bytes()[:i]))
			checkError(t, fmt.Sprintf("Scan of %d of %d bytes", i, buf.Len()), tp.Scan(r, &v2))
		}
	})

	// every write cut short must fail
	t.Run(fmt.Sprintf("%s/write_truncated%s", tp.String(), caller), func(t *testing.T) {
		var buf bytes.Buffer
		checkNoError(t, "Write", tp.Write(rowbinary.NewWriter(&buf), value))

		for i := 0; i < buf.Len(); i++ {
			var wb bytes.Buffer
			ww := rowbinary.NewWriter(newLimitedWriter(&wb, int64(i)))
			checkError(t, fmt.Sprintf("Write limited to %d of %d bytes", i, buf.Len()), tp.Write(ww, value))
		}
	})

	// compare with clickhouse
	t.Run(fmt.Sprintf("%s/format_RowBinary%s", tp.String(), caller), func(t *testing.T) {
		body, err := ExecLocal(query + " AS value FORMAT RowBinary SETTINGS session_timezone='UTC'")
		checkNoError(t, "ExecLocal", err)

		r := rowbinary.NewFormatReader(bytes.NewReader(body), rowbinary.C("value", tp))
		var v T
		checkNoError(t, "Scan", rowbinary.Scan(r, tp, &v))
		checkEqual(t, "Scan result", value, v)

		var buf bytes.Buffer
		w := rowbinary.NewFormatWriter(&buf, rowbinary.C("value", tp))
		checkNoError(t, "Write", rowbinary.Write(w, tp, value))
		checkEqual(t, "Write output", body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNames%s", tp.String(), caller), func(t *testing.T) {
		body, err := ExecLocal(query + " AS value FORMAT RowBinaryWithNames SETTINGS session_timezone='UTC'")
		checkNoError(t, "ExecLocal", err)

		r := rowbinary.NewFormatReader(bytes.NewReader(body), rowbinary.RowBinaryWithNames, rowbinary.C("value", tp))
		var v T
		checkNoError(t, "Scan", rowbinary.Scan(r, tp, &v))
		checkEqual(t, "Scan result", value, v)

		var buf bytes.Buffer
		w := rowbinary.NewFormatWriter(&buf, rowbinary.RowBinaryWithNames, rowbinary.C("value", tp))
		checkNoError(t, "Write", rowbinary.Write(w, tp, value))
		checkEqual(t, "Write output", body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNamesAndTypes_binary%s", tp.String(), caller), func(t *testing.T) {
		body, err := ExecLocal(
			query + namesAndTypesBinarySuffix,
		)
		checkNoError(t, "ExecLocal", err)

		r := rowbinary.NewFormatReader(bytes.NewReader(body), rowbinary.RowBinaryWithNamesAndTypes, rowbinary.WithUseBinaryHeader(true))
		var v T
		checkNoError(t, "Scan", rowbinary.Scan(r, tp, &v))
		checkEqual(t, "Scan result", value, v)

		var buf bytes.Buffer
		w := rowbinary.NewFormatWriter(&buf, rowbinary.RowBinaryWithNamesAndTypes, rowbinary.WithUseBinaryHeader(true), rowbinary.C("value", tp))
		checkNoError(t, "Write", rowbinary.Write(w, tp, value))
		checkEqual(t, "Write output", body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNamesAndTypes_plain%s", tp.String(), caller), func(t *testing.T) {
		body, err := ExecLocal(
			query + namesAndTypesPlainSuffix,
		)
		checkNoError(t, "ExecLocal", err)

		r := rowbinary.NewFormatReader(bytes.NewReader(body), rowbinary.RowBinaryWithNamesAndTypes, rowbinary.WithUseBinaryHeader(false))
		var v T
		checkNoError(t, "Scan", rowbinary.Scan(r, tp, &v))
		checkEqual(t, "Scan result", value, v)

		var buf bytes.Buffer
		w := rowbinary.NewFormatWriter(&buf, rowbinary.RowBinaryWithNamesAndTypes, rowbinary.WithUseBinaryHeader(false), rowbinary.C("value", tp))
		checkNoError(t, "Write", rowbinary.Write(w, tp, value))
		checkEqual(t, "Write output", body, buf.Bytes())
	})

	// insert into clickhouse
	t.Run(fmt.Sprintf("%s/insert_RowBinary%s", tp.String(), caller), func(t *testing.T) {
		if dsn == "" {
			t.Skip("offline mode: live insertion requires an explicit DSN")
		}
		if tp.String() == "Nullable(Nothing)" {
			t.Skip("ClickHouse cannot create a Nullable(Nothing) table column")
		}

		c := NewTestClientTB(t, dsn)

		err := c.Exec(t.Context(), fmt.Sprintf("CREATE TABLE tmp (value %s) ENGINE = Memory", tp.String()))
		checkNoError(t, "CREATE TABLE", err)

		// insert data
		err = c.Insert(t.Context(), "tmp",
			rowbinary.WithColumn("value", tp),
			rowbinary.WithFormatWriter(func(w *rowbinary.FormatWriter) error {
				return rowbinary.Write(w, tp, value)
			}))
		checkNoError(t, "Insert", err)

		// read the inserted row back and compare the value, not only the count
		rows := 0
		err = c.Select(t.Context(), "SELECT value FROM tmp", rowbinary.C("value", tp), rowbinary.WithFormatReader(func(r *rowbinary.FormatReader) error {
			for r.Next() {
				var v T
				if err := rowbinary.Scan(r, tp, &v); err != nil {
					return err
				}
				checkEqual(t, "inserted value", value, v)
				rows++
			}
			return r.Err()
		}))
		checkNoError(t, "Select", err)
		checkEqual(t, "inserted rows", 1, rows)
	})
}

// limitedWriter wraps an io.Writer and limits the total bytes written.
type limitedWriter struct {
	W     io.Writer // The underlying writer
	N     int64     // The maximum number of bytes allowed to be written
	total int64     // The total number of bytes written so far
}

func newLimitedWriter(w io.Writer, limit int64) *limitedWriter {
	return &limitedWriter{
		W: w,
		N: limit,
	}
}

// Write writes bytes to the underlying writer, up to the remaining limit.
func (lw *limitedWriter) Write(p []byte) (int, error) {
	if lw.total >= lw.N {
		return 0, io.EOF // Limit reached, return EOF
	}

	pp := p

	remaining := lw.N - lw.total
	if int64(len(p)) > remaining {
		pp = p[:remaining] // Truncate the buffer if it exceeds the remaining limit
	}

	n, err := lw.W.Write(pp)
	lw.total += int64(n)
	if err != nil {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

// BenchmarkType benchmarks tp's Write and Scan for value.
func BenchmarkType[T any](b *testing.B, tp rowbinary.Type[T], value T) {
	_, file, no, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		no = 0
	}
	caller := fmt.Sprintf("(%s:%d)", filepath.Base(file), no)

	b.Run(fmt.Sprintf("%s/Write%s", tp.String(), caller), func(b *testing.B) {
		out := rowbinary.NewWriter(io.Discard)
		for b.Loop() {
			tp.Write(out, value)
		}
	})

	b.Run(fmt.Sprintf("%s/Scan%s", tp.String(), caller), func(b *testing.B) {
		buf := new(bytes.Buffer)
		out := rowbinary.NewWriter(buf)
		for range 1000 {
			tp.Write(out, value)
		}
		data := buf.Bytes()

		br := bytes.NewReader(data)
		r := rowbinary.NewReader(br)

		var v T

		b.ResetTimer()

		for b.Loop() {
			tp.Scan(r, &v)
			if br.Len() == 0 {
				br.Reset(data)
			}
		}
	})
}
