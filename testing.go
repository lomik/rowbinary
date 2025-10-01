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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testingUnknownType struct{}

// requests clickhouse, caching locally to disk
// re-running the test can already work without CH. including in CI if you commit fixtures/*
func ExecLocal(query string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(query))
	key := fmt.Sprintf("%x", h.Sum(nil))
	filename := fmt.Sprintf("fixtures/ch_%s.bin", key)

	// fmt.Println(filename, query)

	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		body, err := exec.Command("clickhouse", "local", "--query", query).Output()
		if err != nil {
			return nil, err
		}

		err = os.WriteFile(filename, body, 0600)
		return body, err
	}
	// #nosec G304
	return os.ReadFile(filename)
}

var testClientCounter atomic.Uint64

type testClient struct {
	Client
	db string
}

func NewTestClient(ctx context.Context, dsn string, options ...ClientOption) Client {
	db := fmt.Sprintf("db_%d_%d", testClientCounter.Add(1), time.Now().UnixNano())
	defaultClient := NewClient(context.Background(), dsn, append(options, WithDatabase("default"))...)

	err := defaultClient.Exec(context.Background(), "CREATE DATABASE "+db)
	if err != nil {
		log.Fatal(err)
	}
	defaultClient.Close()

	return &testClient{
		Client: NewClient(ctx, dsn, append(options, WithDatabase(db))...),
		db:     db,
	}
}

func (tc *testClient) Close() error {
	return tc.Exec(context.Background(), "DROP DATABASE "+tc.db)
}

func TestType[T any](t *testing.T, tp Type[T], value T, query string) {
	// simple write/read test
	t.Run(fmt.Sprintf("%s/write_read", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.Write(w, value))

		// read
		r := NewReader(bytes.NewReader(buf.Bytes()))
		v2, err := tp.Read(r)
		assert.NoError(err)
		assert.Equal(value, v2)

	})

	// write/read any test
	t.Run(fmt.Sprintf("%s/write_read_any", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.Write(w, value))

		// read
		r := NewReader(bytes.NewReader(buf.Bytes()))
		v2, err := tp.ReadAny(r)
		assert.NoError(err)
		assert.Equal(value, v2)
	})

	// write/read any test
	t.Run(fmt.Sprintf("%s/write_any_read", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.WriteAny(w, value))

		// read
		r := NewReader(bytes.NewReader(buf.Bytes()))
		v2, err := tp.Read(r)
		assert.NoError(err)
		assert.Equal(value, v2)
	})

	// write any wrong type
	t.Run(fmt.Sprintf("%s/write_any_wrong_type", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.True(tp.WriteAny(w, "hello") != nil || tp.WriteAny(w, 42) != nil)
	})

	// write any wrong type
	t.Run(fmt.Sprintf("%s/write_any_wrong_type", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.Error(tp.WriteAny(w, testingUnknownType{}))
	})

	// read truncated
	t.Run(fmt.Sprintf("%s/read_truncated", tp.String()), func(t *testing.T) {
		assert := assert.New(t)

		// write
		var buf bytes.Buffer
		w := NewWriter(&buf)
		assert.NoError(tp.WriteAny(w, value))

		// read
		for i := 0; i < buf.Len()-1; i++ {
			r := NewReader(bytes.NewReader(buf.Bytes()[:i]))
			_, err := tp.Read(r)
			assert.Error(err)
		}
	})

	// write truncated
	t.Run(fmt.Sprintf("%s/write_truncated", tp.String()), func(t *testing.T) {
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
	t.Run(fmt.Sprintf("%s/format_RowBinary", tp.String()), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(query + " AS value FORMAT RowBinary SETTINGS session_timezone='UTC'")
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), C("value", tp))
		v, err := Read(r, tp)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNames", tp.String()), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(query + " AS value FORMAT RowBinaryWithNames SETTINGS session_timezone='UTC'")
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), RowBinaryWithNames, C("value", tp))
		v, err := Read(r, tp)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNames, C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
	})

	t.Run(fmt.Sprintf("%s/format_RowBinaryWithNamesAndTypes_binary", tp.String()), func(t *testing.T) {
		assert := assert.New(t)
		body, err := ExecLocal(
			query + ` AS value FORMAT RowBinaryWithNamesAndTypes 
					SETTINGS 
						output_format_binary_encode_types_in_binary_format=1, 
						session_timezone='UTC'`,
		)
		assert.NoError(err)

		r := NewFormatReader(bytes.NewReader(body), RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true))
		v, err := Read(r, tp)
		assert.NoError(err)
		assert.Equal(value, v)

		var buf bytes.Buffer
		w := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true), C("value", tp))
		assert.NoError(Write(w, tp, value))

		assert.Equal(body, buf.Bytes())
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
