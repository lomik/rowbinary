package rowbinary

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_TypedBinary(t *testing.T) {
	t.Parallel()

	for _, tt := range commonTestData {
		tt := tt

		t.Run(fmt.Sprintf("%s/%#v binary header", tt.tp, tt.want), func(t *testing.T) {
			t.Parallel()
			assert := assert.New(t)

			body, err := execLocal(tt.query + " AS value FORMAT RowBinaryWithNamesAndTypes SETTINGS session_timezone='UTC', output_format_binary_encode_types_in_binary_format=true, input_format_binary_decode_types_in_binary_format=true")
			assert.NoError(err)

			r := NewReader(bytes.NewReader(body))

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

			typeBinary := tt.tp.Binary()
			headerBinary := make([]byte, len(typeBinary))

			nn, err := r.Read(headerBinary)
			if !assert.Equal(len(typeBinary), nn) {
				return
			}
			if !assert.NoError(err) {
				return
			}

			if !assert.Equal(tt.tp.Binary(), headerBinary) {
				return
			}

			valueBody, err := io.ReadAll(r)
			if !assert.NoError(err) {
				return
			}

			valueReader := NewReader(bytes.NewReader(valueBody))

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
				err = tt.tp.WriteAny(NewWriter(w), tt.want)
				if assert.NoError(err) {
					assert.Equal(valueBody, w.Bytes())
				}
			}

			// And if you give one byte less, then there should be an error
			valueReaderTruncated := NewReader(bytes.NewReader(valueBody[:len(valueBody)-1]))

			_, err = tt.tp.ReadAny(valueReaderTruncated)
			assert.Error(err)
		})
	}
}

func Test_DecodeBinaryType(t *testing.T) {
	t.Parallel()

	for _, tt := range commonTestData {
		tt := tt

		t.Run(fmt.Sprintf("%s/%#v binary header", tt.tp, tt.want), func(t *testing.T) {
			t.Parallel()
			assert := assert.New(t)

			body, err := execLocal(tt.query + " AS value FORMAT RowBinaryWithNamesAndTypes SETTINGS session_timezone='UTC', output_format_binary_encode_types_in_binary_format=true, input_format_binary_decode_types_in_binary_format=true")
			assert.NoError(err)

			r := NewReader(bytes.NewReader(body))

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

			tp, err := DecodeBinaryType(r)
			if !assert.NoError(err) {
				return
			}

			if !assert.Equal(tt.tp.Binary(), tp.Binary()) {
				return
			}
		})
	}
}
