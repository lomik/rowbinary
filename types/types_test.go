package types

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

// requests clickhouse, caching locally to disk
// re-running the test can already work without KH. including in CI if you commit fixtures/*
func execLocal(query string) ([]byte, error) {
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

func pointer[V any](v V) *V {
	return &v
}

func null[V any](_ V) *V {
	return nil
}

func Test_Typed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		tp    Any
		want  interface{}
		query string
	}{
		{String, "hello world", "SELECT toString('hello world')"},
		{String, "", "SELECT toString('')"},
		{UInt8, uint8(42), "SELECT toUInt8(42)"},
		{UInt16, uint16(42), "SELECT toUInt16(42)"},
		{UInt32, uint32(42), "SELECT toUInt32(42)"},
		{UInt64, uint64(42), "SELECT toUInt64(42)"},
		{Int8, int8(42), "SELECT toInt8(42)"},
		{Int16, int16(42), "SELECT toInt16(42)"},
		{Int32, int32(42), "SELECT toInt32(42)"},
		{Int64, int64(42), "SELECT toInt64(42)"},
		{Int8, int8(-42), "SELECT toInt8(-42)"},
		{Int16, int16(-42), "SELECT toInt16(-42)"},
		{Int32, int32(-42), "SELECT toInt32(-42)"},
		{Int64, int64(-42), "SELECT toInt64(-42)"},
		{Float64, float64(123.123), "SELECT toFloat64(123.123)"},
		{Float32, float32(123.123), "SELECT toFloat32(123.123)"},
		{Array(UInt32), []uint32{3123213123, 42, 0}, "SELECT [toUInt32(3123213123), toUInt32(42), toUInt32(0)]"},
		{Array(String), []string{"hello world", "epta", ""}, "SELECT ['hello world', 'epta', '']"},
		{Array(Int64), []int64{123123123213123, -2, 0}, "SELECT [toInt64(123123123213123), toInt64(-2), toInt64(0)]"},
		{UUID, uuid.MustParse("258b07b7-daa1-4c80-8062-58a2e07c2601"), "SELECT toUUID('258b07b7-daa1-4c80-8062-58a2e07c2601')"},
		{Decimal(9, 4), decimal.New(42000, -4), "SELECT toDecimal32(4.2, 4)"},
		{Decimal(9, 4), decimal.New(-42000, -4), "SELECT toDecimal32(-4.2, 4)"},
		{Decimal(18, 4), decimal.New(42000, -4), "SELECT toDecimal64(4.2, 4)"},
		{Decimal(18, 4), decimal.New(-42000, -4), "SELECT toDecimal64(-4.2, 4)"},
		{Map(String, String), map[string]string{"key": "value", "key2": "value2"}, "SELECT map('key', 'value', 'key2', 'value2')"},
		{Nullable(Int32), pointer(int32(-42)), "SELECT toNullable(toInt32(-42))"},
		{Nullable(Int32), null(int32(-42)), "SELECT nullIf(toInt32(-42), toInt32(-42))"},
		{DateTime, time.Date(2023, 11, 22, 20, 49, 31, 0, time.UTC), "SELECT toDateTime('2023-11-22 20:49:31')"},
		{Date, time.Date(2023, 11, 22, 0, 0, 0, 0, time.UTC), "SELECT toDate('2023-11-22')"},
		{Date, time.Date(2023, 3, 5, 0, 0, 0, 0, time.UTC), "SELECT toDate('2023-03-05')"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(fmt.Sprintf("%s/%#v", tt.tp, tt.want), func(t *testing.T) {
			t.Parallel()
			assert := assert.New(t)

			body, err := execLocal(tt.query + " AS value FORMAT RowBinaryWithNamesAndTypes SETTINGS session_timezone='UTC'")
			assert.NoError(err)

			r := bytes.NewReader(body)

			// first read the column name and type. check that there is one column and the type matches the one being checked
			n, err := UVarint.Read(r)
			if !assert.NoError(err) {
				return
			}

			if !assert.Equal(uint64(1), n) {
				return
			}

			name, err := String.Read(r)
			if !assert.NoError(err) {
				return
			}

			if !assert.Equal("value", name) {
				return
			}

			tp, err := String.Read(r)
			if !assert.NoError(err) {
				return
			}

			if !assert.Equal(tt.tp.String(), tp) {
				return
			}

			valueBody, err := io.ReadAll(r)
			if !assert.NoError(err) {
				return
			}

			valueReader := bytes.NewReader(valueBody)

			value, err := tt.tp.ReadAny(valueReader)
			if assert.NoError(err) {
				assert.Equal(tt.want, value)
			}

			tail, err := io.ReadAll(valueReader)
			if assert.NoError(err) {
				assert.Equal([]byte{}, tail)
			}

			// Now let's check that writer generates the same thing
			if strings.HasPrefix(tt.tp.String(), "Map(") {
				// pass
				// once ok
				// 	ok := false
				// mapLoop:
				// 	for i := 0; i < 1000; i++ {
				// 		w := new(bytes.Buffer)
				// 		err = tt.tp.WriteAny(w, tt.want)
				// 		if assert.NoError(err) {
				// 			if bytes.Equal(valueBody, w.Bytes()) {
				// 				ok = true
				// 				break mapLoop
				// 			}
				// 		}
				// 	}
				// 	assert.Equal(true, ok)
			} else {
				w := new(bytes.Buffer)
				err = tt.tp.WriteAny(w, tt.want)
				if assert.NoError(err) {
					assert.Equal(valueBody, w.Bytes())
				}
			}

			// And if you give one byte less, then there should be an error
			valueReaderTruncated := bytes.NewReader(valueBody[:len(valueBody)-1])

			_, err = tt.tp.ReadAny(valueReaderTruncated)
			assert.Error(err)
		})
	}
}

func TestReader_Uvarint(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	body, err := execLocal("SELECT 1 as c1, 2 as c2, 3 as c3, 4 as c4 FORMAT RowBinaryWithNamesAndTypes")
	assert.NoError(err)

	r := bytes.NewReader(body)

	value, err := UVarint.Read(r)
	assert.NoError(err)
	assert.Equal(uint64(4), value)
}
