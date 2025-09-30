package rowbinary

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatWriter_NewFormatWriter(t *testing.T) {
	t.Parallel()

	t.Run("default options", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf)

		assert.NotNil(writer)
		assert.Equal(RowBinary, writer.options.format)
		assert.Nil(writer.options.columns)
		assert.False(writer.options.useBinaryHeader)
		assert.False(writer.doneInit)
		assert.Nil(writer.firstErr)
	})

	t.Run("with options", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinaryWithNames, C("test", String), WithUseBinaryHeader(true))

		assert.NotNil(writer)
		assert.Equal(RowBinaryWithNames, writer.options.format)
		assert.Len(writer.options.columns, 1)
		assert.Equal("test", writer.options.columns[0].name)
		assert.True(writer.options.useBinaryHeader)
	})
}

func TestFormatWriter_WriteHeader(t *testing.T) {
	t.Parallel()

	t.Run("RowBinary - no header", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary, C("col", UInt32))

		err := writer.WriteHeader()
		assert.NoError(err)
		assert.True(writer.doneInit)
		assert.Empty(buf.Bytes()) // No header written for RowBinary
	})

	t.Run("RowBinaryWithNames", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinaryWithNames, C("col1", UInt32), C("col2", String))

		err := writer.WriteHeader()
		assert.NoError(err)
		assert.True(writer.doneInit)

		// Verify header: number of columns (2), then names "col1", "col2"
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		numCols, err := UVarint.Read(reader)
		assert.NoError(err)
		assert.Equal(uint64(2), numCols)

		name1, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col1", name1)

		name2, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col2", name2)
	})

	t.Run("RowBinaryWithNamesAndTypes - string types", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithColumn("col1", UInt32), WithColumn("col2", String))

		err := writer.WriteHeader()
		assert.NoError(err)
		assert.True(writer.doneInit)

		// Verify header: number of columns (2), names "col1", "col2", types "UInt32", "String"
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		numCols, err := UVarint.Read(reader)
		assert.NoError(err)
		assert.Equal(uint64(2), numCols)

		name1, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col1", name1)

		name2, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col2", name2)

		type1, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("UInt32", type1)

		type2, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("String", type2)
	})

	t.Run("RowBinaryWithNamesAndTypes - binary types", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true), WithColumn("col1", UInt32), WithColumn("col2", String))

		err := writer.WriteHeader()
		assert.NoError(err)
		assert.True(writer.doneInit)

		// Verify header: number of columns (2), names "col1", "col2", binary types
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		numCols, err := UVarint.Read(reader)
		assert.NoError(err)
		assert.Equal(uint64(2), numCols)

		name1, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col1", name1)

		name2, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("col2", name2)

		// Read binary types
		typeBytes := make([]byte, 1)
		_, err = reader.Read(typeBytes)
		assert.NoError(err)
		assert.Equal(BinaryTypeUInt32[0], typeBytes[0])

		_, err = reader.Read(typeBytes)
		assert.NoError(err)
		assert.Equal(BinaryTypeString[0], typeBytes[0])
	})

	t.Run("no columns defined", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary) // no columns

		err := writer.WriteHeader()
		assert.Error(err)
		assert.Contains(err.Error(), "no columns defined in options")
		assert.Equal(err, writer.Err())
	})
}

func TestFormatWriter_WriteAny(t *testing.T) {
	t.Parallel()

	t.Run("successful write", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary, C("num", UInt32), C("str", String))

		err := writer.WriteAny(uint32(42), "hello")
		assert.NoError(err)

		// Verify written data
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		num, err := UInt32.Read(reader)
		assert.NoError(err)
		assert.Equal(uint32(42), num)

		str, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("hello", str)
	})

	t.Run("multiple calls cycle through columns", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary, C("a", UInt32), C("b", String))

		// First row
		err := writer.WriteAny(uint32(1), "first")
		assert.NoError(err)

		// Second row
		err = writer.WriteAny(uint32(2), "second")
		assert.NoError(err)

		// Verify data
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		a1, err := UInt32.Read(reader)
		assert.NoError(err)
		assert.Equal(uint32(1), a1)

		b1, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("first", b1)

		a2, err := UInt32.Read(reader)
		assert.NoError(err)
		assert.Equal(uint32(2), a2)

		b2, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("second", b2)
	})

	t.Run("write after error", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Use a writer that will fail
		failingWriter := &failingWriter{}
		writer := NewFormatWriter(failingWriter, RowBinary, C("num", UInt32))

		// First write fails
		err := writer.WriteAny(uint32(42))
		assert.Error(err)

		// Subsequent writes should return the same error
		err2 := writer.WriteAny(uint32(43))
		assert.Equal(err, err2)
		assert.Equal(writer.Err(), err)
	})
}

func TestFormatWriter_Write(t *testing.T) {
	t.Parallel()

	t.Run("successful write", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary, C("num", UInt32), C("str", String))

		err := Write(writer, UInt32, uint32(42))
		assert.NoError(err)

		err = Write(writer, String, "hello")
		assert.NoError(err)

		// Verify written data
		reader := NewReader(bytes.NewReader(buf.Bytes()))
		num, err := UInt32.Read(reader)
		assert.NoError(err)
		assert.Equal(uint32(42), num)

		str, err := String.Read(reader)
		assert.NoError(err)
		assert.Equal("hello", str)
	})

	t.Run("type mismatch", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewFormatWriter(&buf, RowBinary, C("num", UInt32))

		err := Write(writer, String, "hello") // wrong type
		assert.Error(err)
		assert.Contains(err.Error(), "type mismatch")
		assert.Contains(err.Error(), "expected UInt32, got String")
	})

	t.Run("write after error", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		failingWriter := &failingWriter{}
		writer := NewFormatWriter(failingWriter, RowBinary, C("num", UInt32))

		// First write fails
		err := Write(writer, UInt32, uint32(42))
		assert.Error(err)

		// Subsequent writes should return the same error
		err2 := Write(writer, UInt32, uint32(43))
		assert.Equal(err, err2)
	})
}

func TestFormatWriter_Errors(t *testing.T) {
	t.Parallel()

	t.Run("write error propagates", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		failingWriter := &failingWriter{}
		writer := NewFormatWriter(failingWriter, RowBinaryWithNames, C("col", UInt32))

		// WriteHeader should fail due to failing writer
		err := writer.WriteHeader()
		assert.Error(err)
		assert.Equal(err, writer.Err())
	})

	t.Run("first error preserved", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		failingWriter := &failingWriter{}
		writer := NewFormatWriter(failingWriter, RowBinary, C("col", UInt32))

		// First operation fails
		err1 := writer.WriteAny(uint32(42))
		assert.Error(err1)

		// Second operation also fails, but should return the first error
		err2 := writer.WriteAny(uint32(43))
		assert.Equal(err1, err2)
		assert.Equal(writer.Err(), err1)
	})
}

// failingWriter is a helper that always returns an error on write
type failingWriter struct{}

func (f *failingWriter) Write(p []byte) (n int, err error) {
	return 0, errors.New("write failed")
}
