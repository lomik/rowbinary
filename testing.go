package rowbinary

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testClickHouseDSN = "http://127.0.0.1:8123"

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
		body, err := exec.Command("clickhouse", "local", "--query", query).Output()
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
// Note: The function will log.Fatal if database creation fails. The returned client should be
// closed using its Close method to drop the database.
func NewTestClient(ctx context.Context, dsn string, options ...ClientOption) *TestClient {
	db := fmt.Sprintf("db_%d_%d", testClientCounter.Add(1), time.Now().UnixNano())
	c := NewClient(ctx, append(options, WithDSN(dsn), WithDatabase(db))...)

	err := c.Exec(context.Background(), "CREATE DATABASE "+db, WithDatabase("default"))
	if err != nil {
		log.Fatal(err)
	}

	return &TestClient{
		Client: c,
		db:     db,
	}
}

func (tc *TestClient) Close() error {
	return tc.Exec(context.Background(), "DROP DATABASE "+tc.db)
}

func (tc *TestClient) Database() string {
	return tc.db
}

func TestType[T any](t *testing.T, tp Type[T], value T, query string) {
	_, file, no, ok := runtime.Caller(1)
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

	// write any wrong type
	t.Run(fmt.Sprintf("%s/write_any_wrong_type%s", tp.String(), caller), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.True(tp.WriteAny(w, "hello") != nil || tp.WriteAny(w, 42) != nil)
	})

	// write any wrong type
	t.Run(fmt.Sprintf("%s/write_any_wrong_type%s", tp.String(), caller), func(t *testing.T) {
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
		for i := 0; i < buf.Len()-1; i++ {
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
		for i := 0; i < buf.Len()-1; i++ {
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
		if tp.String() == "Nullable(Nothing)" {
			return
		}

		assert := assert.New(t)
		c := NewTestClient(t.Context(), testClickHouseDSN)
		defer c.Close()

		err := c.Exec(t.Context(), fmt.Sprintf("CREATE TABLE tmp (value %s) ENGINE = Memory", tp.String()))
		assert.NoError(err)

		// insert data
		err = c.Insert(t.Context(), "tmp",
			WithColumn("value", tp),
			WithFormatWriter(func(w *FormatWriter) error {
				return Write(w, tp, value)
			}))
		assert.NoError(err)

		// check inserted
		err = c.Select(t.Context(), "SELECT count() FROM tmp", WithFormatReader(func(r *FormatReader) error {
			var v uint64
			assert.NoError(Scan(r, UInt64, &v))
			assert.Equal(uint64(1), v)
			return nil
		}))
		assert.NoError(err)
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
