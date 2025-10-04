package rowbinary

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatReader_NewFormatReader(t *testing.T) {
	t.Parallel()

	t.Run("default options", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		buf := bytes.NewReader([]byte{})
		reader := NewFormatReader(buf)

		assert.NotNil(reader)
		assert.Equal(RowBinary, reader.options.format)
		assert.Nil(reader.options.columns)
		assert.False(reader.options.useBinaryHeader)
	})

	t.Run("with options", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		buf := bytes.NewReader([]byte{})
		reader := NewFormatReader(buf, RowBinaryWithNames, WithColumn("test", String), WithUseBinaryHeader(true))

		assert.NotNil(reader)
		assert.Equal(RowBinaryWithNames, reader.options.format)
		assert.Len(reader.options.columns, 1)
		assert.Equal("test", reader.options.columns[0].name)
		assert.True(reader.options.useBinaryHeader)
	})
}

func TestFormatReader_RowBinary(t *testing.T) {
	t.Parallel()

	t.Run("successful read", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Create data: uint32(42), string("hello")
		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "hello"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			WithColumn("num", UInt32),
			WithColumn("str", String))

		// First read
		assert.True(reader.Next())
		num, err := Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(42), num)

		str, err := Read(reader, String)
		assert.NoError(err)
		assert.Equal("hello", str)

		// IOF
		_, err = reader.ReadAny()
		assert.Error(err)
		assert.True(errors.Is(err, io.EOF))

		// Next should return false (no more rows)
		assert.False(reader.Next())
	})

	t.Run("missing columns option", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		buf := bytes.NewReader([]byte{1, 2, 3})
		reader := NewFormatReader(buf, RowBinary) // no columns specified

		assert.False(reader.Next())
		assert.Error(reader.Err())
		assert.Contains(reader.Err().Error(), "columns must be set")
	})
}

func TestFormatReader_RowBinaryWithNames(t *testing.T) {
	t.Parallel()

	t.Run("successful read", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Create header: 2 columns, names "num", "str"
		// Data: uint32(42), string("hello")
		var buf bytes.Buffer
		writer := NewWriter(&buf)

		// Write number of columns
		require.NoError(t, UVarint.Write(writer, 2))
		// Write column names
		require.NoError(t, String.Write(writer, "num"))
		require.NoError(t, String.Write(writer, "str"))
		// Write data
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "hello"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinaryWithNames,
			WithColumn("num", UInt32),
			WithColumn("str", String))

		assert.True(reader.Next())
		num, err := Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(42), num)

		str, err := Read(reader, String)
		assert.NoError(err)
		assert.Equal("hello", str)
	})

	t.Run("unknown column name", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UVarint.Write(writer, 1))
		require.NoError(t, String.Write(writer, "unknown"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinaryWithNames,
			C("num", UInt32))

		assert.False(reader.Next())
		assert.Error(reader.Err())
		assert.Contains(reader.Err().Error(), "type for column unknown is not defined")
	})
}

func TestFormatReader_RowBinaryWithNamesAndTypes(t *testing.T) {
	t.Parallel()

	t.Run("successful read with binary types", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Create header: 2 columns, names "num", "str", types UInt32, String
		// Data: uint32(42), string("hello")
		var buf bytes.Buffer
		writer := NewWriter(&buf)

		// Write number of columns
		require.NoError(t, UVarint.Write(writer, 2))
		// Write column names
		require.NoError(t, String.Write(writer, "num"))
		require.NoError(t, String.Write(writer, "str"))
		// Write types (binary)
		require.NoError(t, writer.WriteByte(BinaryTypeUInt32[0]))
		require.NoError(t, writer.WriteByte(BinaryTypeString[0]))
		// Write data
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "hello"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinaryWithNamesAndTypes,
			WithUseBinaryHeader(true))

		assert.True(reader.Next())
		num, err := Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(42), num)

		str, err := Read(reader, String)
		assert.NoError(err)
		assert.Equal("hello", str)
	})

	t.Run("type mismatch", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)

		require.NoError(t, UVarint.Write(writer, 1))
		require.NoError(t, String.Write(writer, "num"))
		require.NoError(t, writer.WriteByte(BinaryTypeUInt32[0]))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinaryWithNamesAndTypes,
			C("num", String), // wrong type
			WithUseBinaryHeader(true))

		assert.False(reader.Next())
		assert.Error(reader.Err())
		assert.Contains(reader.Err().Error(), "mismatched column type")
	})
}

func TestFormatReader_Next(t *testing.T) {
	t.Parallel()

	t.Run("multiple rows", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Create data for 2 rows: each with uint32(1), uint32(2)
		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 1))
		require.NoError(t, UInt32.Write(writer, 2))
		require.NoError(t, UInt32.Write(writer, 3))
		require.NoError(t, UInt32.Write(writer, 4))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("a", UInt32),
			C("b", UInt32))

		// Row 1
		assert.True(reader.Next())
		a, err := Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(1), a)
		b, err := Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(2), b)

		// Row 2
		assert.True(reader.Next())
		a, err = Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(3), a)
		b, err = Read(reader, UInt32)
		assert.NoError(err)
		assert.Equal(uint32(4), b)

		// No more rows
		assert.False(reader.Next())
	})

	t.Run("EOF detection", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Empty data
		reader := NewFormatReader(bytes.NewReader([]byte{}),
			RowBinary,
			C("a", UInt32))

		assert.False(reader.Next())
		assert.NoError(reader.Err()) // EOF is not an error for Next
	})
}

func TestFormatReader_ReadAny(t *testing.T) {
	t.Parallel()

	t.Run("successful read", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "test"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("num", UInt32),
			C("str", String))

		assert.True(reader.Next())
		val1, err := reader.ReadAny()
		assert.NoError(err)
		assert.Equal(uint32(42), val1)

		val2, err := reader.ReadAny()
		assert.NoError(err)
		assert.Equal("test", val2)
	})

	t.Run("type mismatch in Read", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 42))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("num", UInt32))

		assert.True(reader.Next())
		_, err := Read(reader, String) // wrong type
		assert.Error(err)
		assert.Contains(err.Error(), "type mismatch")
	})
}

func TestFormatReader_Scan(t *testing.T) {
	t.Parallel()

	t.Run("successful scan", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "test"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("num", UInt32),
			C("str", String))

		assert.True(reader.Next())
		var val1 uint32
		err := Scan(reader, UInt32, &val1)
		assert.NoError(err)
		assert.Equal(uint32(42), val1)

		var val2 string
		err = Scan(reader, String, &val2)
		assert.NoError(err)
		assert.Equal("test", val2)
	})
}

func TestFormatReader_ScanAny(t *testing.T) {
	t.Parallel()

	t.Run("successful scan", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, UInt32.Write(writer, 42))
		require.NoError(t, String.Write(writer, "test"))

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("num", UInt32),
			C("str", String))

		assert.True(reader.Next())
		var val1 uint32
		err := reader.ScanAny(&val1)
		assert.NoError(err)
		assert.Equal(uint32(42), val1)

		var val2 string
		err = reader.ScanAny(&val2)
		assert.NoError(err)
		assert.Equal("test", val2)
	})
}

func TestFormatReader_Errors(t *testing.T) {
	t.Parallel()

	t.Run("truncated data", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Write incomplete string (length 5 but only 3 bytes)
		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, writer.WriteByte(5)) // string length
		_, err := writer.Write([]byte("hel"))   // incomplete
		require.NoError(t, err)

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("str", String))

		assert.True(reader.Next())
		_, err = reader.ReadAny()
		assert.Error(err)
		assert.Error(reader.Err())
	})

	t.Run("invalid format", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		buf := bytes.NewReader([]byte{})
		reader := NewFormatReader(buf, Format(999)) // invalid format

		assert.False(reader.Next())
		assert.Error(reader.Err())
		assert.Contains(reader.Err().Error(), "unknown format")
	})
}

func TestFormatReader_ReadAfterError(t *testing.T) {
	t.Parallel()

	t.Run("read after error", func(t *testing.T) {
		t.Parallel()
		assert := assert.New(t)

		// Truncated data
		var buf bytes.Buffer
		writer := NewWriter(&buf)
		require.NoError(t, writer.WriteByte(5)) // string length
		_, err := writer.Write([]byte("hel"))   // incomplete
		require.NoError(t, err)

		reader := NewFormatReader(bytes.NewReader(buf.Bytes()),
			RowBinary,
			C("str", String))

		assert.True(reader.Next())
		_, err = reader.ReadAny()
		assert.Error(err)

		// Subsequent calls should return the same error
		_, err2 := reader.ReadAny()
		assert.Equal(err, err2)
		assert.Equal(reader.Err(), err)
	})
}
