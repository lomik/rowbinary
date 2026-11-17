package rowbinary

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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testingUnknownType struct{}

// ExecLocal executes a ClickHouse query locally using the 'clickhouse local' command and caches the result to disk.
//
// It takes a query string as input, computes a SHA256 hash of the query to generate a unique filename,
// and stores the result in the 'fixtures/' directory. If the result is already cached, it reads from the file
// instead of re-executing the query. This is primarily used for testing purposes to avoid repeated executions
// of the same query, especially in CI environments where fixtures can be committed.
//
// Parameters:
//   - query: The ClickHouse query string to execute.
//
// Returns:
//   - []byte: The binary output of the query.
//   - error: An error if the command fails or file operations encounter issues.
//
// Note: The function assumes 'clickhouse' is available in the system PATH.
// The query is executed with default settings unless specified in the query string.
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

type TestClient struct {
	Client
	db string
}

// NewTestClient creates a new test client with an isolated database for testing purposes.
//
// It generates a unique database name using an atomic counter and current timestamp,
// creates a new ClickHouse client connected to that database, and executes a CREATE DATABASE
// command to ensure the database exists. The client is configured with the provided DSN and options,
// and the database is set to the newly created one.
//
// Parameters:
//   - ctx: Context for the client creation.
//   - dsn: Data Source Name for connecting to ClickHouse.
//   - options: Optional client configuration options.
//
// Returns:
//   - Client: A test client instance that wraps the standard Client with database isolation.
//     The client automatically manages the database lifecycle.
//
// Note: The function panics if database creation fails. Prefer NewTestClientE or NewTestClientTB.
// The returned client should be
// closed using its Close method to drop the database.
func NewTestClient(ctx context.Context, dsn string, options ...ClientOption) *TestClient {
	c, err := NewTestClientE(ctx, dsn, options...)
	if err != nil {
		panic(err)
	}
	return c
}

// NewTestClientE creates an isolated database, honoring the caller's context.
func NewTestClientE(ctx context.Context, dsn string, options ...ClientOption) (*TestClient, error) {
	db := fmt.Sprintf("db_%d_%d", testClientCounter.Add(1), time.Now().UnixNano())
	c := NewClient(append(options, WithDSN(dsn), WithDatabase(db))...)

	err := c.Exec(ctx, "CREATE DATABASE "+db, WithDatabase("default"))
	if err != nil {
		return nil, fmt.Errorf("create test database: %w", err)
	}

	return &TestClient{
		Client: c,
		db:     db,
	}, nil
}

func (tc *TestClient) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return tc.Exec(ctx, "DROP DATABASE "+tc.db)
}

// NewTestClientTB bounds setup and registers cleanup that reports failures to the test.
func NewTestClientTB(t testing.TB, dsn string, options ...ClientOption) *TestClient {
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

func (tc *TestClient) Database() string {
	return tc.db
}

func TestType[T any](t *testing.T, tp Type[T], value T, query string) {
	t.Helper()
	testTypeAt(t, 2, tp, value, query, "")
}

// TestTypeWithDSN runs fixture checks and, if dsn is nonempty, a live insert check.
// TestType is the Docker-free fixture-only convenience entry point.
func TestTypeWithDSN[T any](t *testing.T, tp Type[T], value T, query, dsn string) {
	t.Helper()
	testTypeAt(t, 2, tp, value, query, dsn)
}

// testTypeAt labels subtests with the source location callerSkip frames above it,
// so wrappers can point the label at the test line instead of themselves.
func testTypeAt[T any](t *testing.T, callerSkip int, tp Type[T], value T, query, dsn string) {
	t.Helper()
	_, file, no, ok := runtime.Caller(callerSkip)
	if !ok {
		file = "unknown"
		no = 0
	}
	caller := fmt.Sprintf("(%s:%d)", filepath.Base(file), no)

	bodyEncoded, err := ExecLocal(query + " AS value FORMAT RowBinary SETTINGS session_timezone='UTC'")
	assert.NoError(t, err)

	// compare Write with clickhouse
	t.Run(fmt.Sprintf("%s/write%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.Write(w, value))
		assert.Equal(bodyEncoded, buf.Bytes())
	})

	// Scan test
	t.Run(fmt.Sprintf("%s/scan%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)
		// scan
		r := NewReader(bytes.NewBuffer(bodyEncoded))
		var v2 T
		err := tp.Scan(r, &v2)
		assert.NoError(err)
		assert.Equal(value, v2)
	})

	// compare WriteAny with clickhouse
	t.Run(fmt.Sprintf("%s/write_any%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.WriteAny(w, value))
		assert.Equal(bodyEncoded, buf.Bytes())
	})

	// write any wrong type: every candidate whose Go type is not T must fail
	t.Run(fmt.Sprintf("%s/write_any_wrong_type%s", tp.String(), caller), func(t *testing.T) {
		for _, v := range []any{"hello", 42} {
			if _, ok := v.(T); ok {
				continue
			}
			var buf bytes.Buffer
			assert.Error(t, tp.WriteAny(NewWriter(&buf), v), "WriteAny(%T)", v)
		}
	})

	// write any of a type no codec accepts
	t.Run(fmt.Sprintf("%s/write_any_unknown_type%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.Error(tp.WriteAny(w, testingUnknownType{}))
	})

	// scan truncated
	t.Run(fmt.Sprintf("%s/scan_truncated%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.WriteAny(w, value))

		// scan
		var v2 T
		for i := 0; i < buf.Len(); i++ {
			r := NewReader(bytes.NewReader(buf.Bytes()[:i]))
			err := tp.Scan(r, &v2)
			assert.Error(err)
		}
	})

	// write truncated
	t.Run(fmt.Sprintf("%s/write_truncated%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.Write(w, value))

		// read
		for i := 0; i < buf.Len(); i++ {
			var wb bytes.Buffer
			ww := NewWriter(newLimitedWriter(&wb, int64(i)))
			err := tp.Write(ww, value)
			assert.Error(err)
		}
	})

	// compare with clickhouse
	t.Run(fmt.Sprintf("%s/format_RowBinary%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(query + " AS value FORMAT RowBinary SETTINGS session_timezone='UTC'")
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), C("value", tp))
		var v T
		err = Scan(r, tp, &v)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNames%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(query + " AS value FORMAT RowBinaryWithNames SETTINGS session_timezone='UTC'")
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), RowBinaryWithNames, C("value", tp))
		var v T
		err = Scan(r, tp, &v)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNames, C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNamesAndTypes_binary%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(
			query + ` AS value FORMAT RowBinaryWithNamesAndTypes 
					SETTINGS 
						output_format_binary_encode_types_in_binary_format=1, 
						session_timezone='UTC'`,
		)
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true))
		var v T
		err = Scan(r, tp, &v)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true), C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNamesAndTypes_plain%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(
			query + ` AS value FORMAT RowBinaryWithNamesAndTypes 
					SETTINGS 
						output_format_binary_encode_types_in_binary_format=0, 
						session_timezone='UTC'`,
		)
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), RowBinaryWithNamesAndTypes, WithUseBinaryHeader(false))
		var v T
		err = Scan(r, tp, &v)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(false), C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	// insert into clickhouse
	t.Run(fmt.Sprintf("%s/insert_RowBinary%s", tp.String(), caller), func(t *testing.T) {
		if dsn == "" {
			t.Skip("offline mode: live insertion requires an explicit DSN")
		}
		if tp.String() == "Nullable(Nothing)" {
			t.Skip("ClickHouse cannot create a Nullable(Nothing) table column")
		}

		assert := assert.New(t)
		c := NewTestClientTB(t, dsn)

		err := c.Exec(t.Context(), fmt.Sprintf("CREATE TABLE tmp (value %s) ENGINE = Memory", tp.String()))
		assert.NoError(err)

		// insert data
		err = c.Insert(t.Context(), "tmp",
			WithColumn("value", tp),
			WithFormatWriter(func(w *FormatWriter) error {
				return Write(w, tp, value)
			}))
		assert.NoError(err)

		// read the inserted row back and compare the value, not only the count
		rows := 0
		err = c.Select(t.Context(), "SELECT value FROM tmp", C("value", tp), WithFormatReader(func(r *FormatReader) error {
			for r.Next() {
				var v T
				if err := Scan(r, tp, &v); err != nil {
					return err
				}
				assert.Equal(value, v)
				rows++
			}
			return r.Err()
		}))
		assert.NoError(err)
		assert.Equal(1, rows)
	})
}

// limitedWriter wraps an io.Writer and limits the total bytes written.
type limitedWriter struct {
	W     io.Writer // The underlying writer
	N     int64     // The maximum number of bytes allowed to be written
	total int64     // The total number of bytes written so far
}

// NewLimitedWriter creates a new LimitedWriter.
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

func BenchmarkType[T any](b *testing.B, tp Type[T], value T) {
	_, file, no, ok := runtime.Caller(1)
	if !ok {
		file = "unknown"
		no = 0
	}
	caller := fmt.Sprintf("(%s:%d)", filepath.Base(file), no)

	b.Run(fmt.Sprintf("%s/Write%s", tp.String(), caller), func(b *testing.B) {
		out := NewWriter(io.Discard)
		for b.Loop() {
			tp.Write(out, value)
		}
	})

	b.Run(fmt.Sprintf("%s/Scan%s", tp.String(), caller), func(b *testing.B) {
		buf := new(bytes.Buffer)
		out := NewWriter(buf)
		for range 1000 {
			tp.Write(out, value)
		}
		data := buf.Bytes()

		br := bytes.NewReader(data)
		r := NewReader(br)

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
