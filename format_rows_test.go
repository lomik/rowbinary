package rowbinary

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
)

type testRow struct {
	id   uint32
	name string
	opt  *int64
	tags []uint8
}

var testRows = func() []testRow {
	v := int64(-7)
	return []testRow{
		{1, "one", &v, []uint8{1, 2}},
		{2, "", nil, nil},
		{3, "three", nil, []uint8{}},
		{4, "four", &v, []uint8{9}},
	}
}()

var testColumns = []FormatOption{C("id", UInt32), C("name", String), C("opt", Nullable(Int64)), C("tags", Array(UInt8))}

type formatCase struct {
	name     string
	write    []FormatOption // format options for the writer (columns are added)
	read     []FormatOption // format options for the reader
	withCols bool           // reader needs the columns from the options
}

var formatCases = []formatCase{
	{"RowBinary", []FormatOption{RowBinary}, []FormatOption{RowBinary}, true},
	{"RowBinaryWithNames", []FormatOption{RowBinaryWithNames}, []FormatOption{RowBinaryWithNames}, true},
	{"RowBinaryWithNamesAndTypes/text", []FormatOption{RowBinaryWithNamesAndTypes}, []FormatOption{RowBinaryWithNamesAndTypes}, false},
	{"RowBinaryWithNamesAndTypes/binary", []FormatOption{RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true)}, []FormatOption{RowBinaryWithNamesAndTypes, WithUseBinaryHeader(true)}, false},
}

// encodeRows writes testRows and returns the stream and the offset at which
// each row ends (the first entry is the end of the header).
func encodeRows(t *testing.T, fc formatCase) ([]byte, []int) {
	t.Helper()
	var buf bytes.Buffer
	w := NewFormatWriter(&buf, append(append([]FormatOption{}, fc.write...), testColumns...)...)
	if err := w.WriteHeader(); err != nil {
		t.Fatal(err)
	}
	ends := []int{buf.Len()}
	for _, row := range testRows {
		if err := Write(w, UInt32, row.id); err != nil {
			t.Fatal(err)
		}
		if err := Write(w, String, row.name); err != nil {
			t.Fatal(err)
		}
		if err := Write(w, Nullable(Int64), row.opt); err != nil {
			t.Fatal(err)
		}
		if err := Write(w, Array(UInt8), row.tags); err != nil {
			t.Fatal(err)
		}
		ends = append(ends, buf.Len())
	}
	return buf.Bytes(), ends
}

func newRowsReader(fc formatCase, data []byte) *FormatReader {
	opts := append([]FormatOption{}, fc.read...)
	if fc.withCols {
		opts = append(opts, testColumns...)
	}
	return NewFormatReader(bytes.NewReader(data), opts...)
}

func readTypedRows(r *FormatReader) ([]testRow, error) {
	var rows []testRow
	for r.Next() {
		var row testRow
		if err := Scan(r, UInt32, &row.id); err != nil {
			return rows, err
		}
		if err := Scan(r, String, &row.name); err != nil {
			return rows, err
		}
		if err := Scan(r, Nullable(Int64), &row.opt); err != nil {
			return rows, err
		}
		if err := Scan(r, Array(UInt8), &row.tags); err != nil {
			return rows, err
		}
		rows = append(rows, row)
	}
	return rows, r.Err()
}

func sameRows(a, b []testRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		x, y := a[i], b[i]
		if x.id != y.id || x.name != y.name || (x.opt == nil) != (y.opt == nil) || (x.opt != nil && *x.opt != *y.opt) || !bytes.Equal(x.tags, y.tags) {
			return false
		}
	}
	return true
}

func TestFormatManyRowsAndColumns(t *testing.T) {
	for _, fc := range formatCases {
		t.Run(fc.name, func(t *testing.T) {
			data, _ := encodeRows(t, fc)
			r := newRowsReader(fc, data)
			rows, err := readTypedRows(r)
			if err != nil || !sameRows(rows, testRows) {
				t.Fatalf("rows = %+v, err %v", rows, err)
			}
			if r.Next() || r.Err() != nil {
				t.Fatalf("after the last row: Next true or Err %v", r.Err())
			}
		})
	}
}

// Scanning one column per call cycles through the columns row by row.
func TestFormatReaderColumnCycling(t *testing.T) {
	fc := formatCases[3]
	data, _ := encodeRows(t, fc)
	r := newRowsReader(fc, data)
	var got []any
	for r.Next() {
		for range 4 {
			var v any
			if err := r.Scan(&v); err != nil {
				t.Fatal(err)
			}
			got = append(got, v)
		}
	}
	if r.Err() != nil || len(got) != 16 {
		t.Fatalf("%d values, err %v", len(got), r.Err())
	}
	if got[4] != uint32(2) || got[5] != "" || got[6] != nil || got[12] != uint32(4) || got[13] != "four" {
		t.Fatalf("values = %v", got)
	}
}

// Cutting the stream at every byte: a cut at a row boundary is a clean end,
// a cut inside a row is io.ErrUnexpectedEOF, a cut in the header is an error.
func TestFormatReaderTruncationAtEveryByte(t *testing.T) {
	for _, fc := range formatCases {
		t.Run(fc.name, func(t *testing.T) {
			data, ends := encodeRows(t, fc)
			for cut := 0; cut <= len(data); cut++ {
				rows, err := readTypedRows(newRowsReader(fc, data[:cut]))
				complete := 0
				boundary := false
				for i, end := range ends[1:] {
					if end <= cut {
						complete = i + 1
					}
				}
				for _, end := range ends {
					boundary = boundary || end == cut
				}
				what := fmt.Sprintf("cut %d of %d", cut, len(data))
				switch {
				case cut < ends[0] && fc.name != "RowBinary":
					if err == nil {
						t.Errorf("%s (in the header): no error", what)
					}
				case boundary:
					if err != nil || len(rows) != complete {
						t.Errorf("%s (row boundary): %d rows, err %v; want %d rows, no error", what, len(rows), err, complete)
					}
				default:
					if !errors.Is(err, io.ErrUnexpectedEOF) || len(rows) != complete {
						t.Errorf("%s (inside a row): %d rows, err %v; want %d rows, io.ErrUnexpectedEOF", what, len(rows), err, complete)
					}
				}
			}
		})
	}
}

func TestFormatReaderHeaderMismatches(t *testing.T) {
	data, _ := encodeRows(t, formatCases[1]) // RowBinaryWithNames: 4 names, no types

	// A column in the header without a type in the options.
	r := NewFormatReader(bytes.NewReader(data), RowBinaryWithNames, C("id", UInt32), C("name", String), C("opt", Nullable(Int64)))
	if r.Next() || r.Err() == nil {
		t.Error("header column without a type: no error")
	}

	// A column whose type differs from the header (types in the header).
	typed, _ := encodeRows(t, formatCases[2])
	r = NewFormatReader(bytes.NewReader(typed), RowBinaryWithNamesAndTypes, C("id", UInt64))
	if r.Next() || r.Err() == nil {
		t.Error("mismatched column type: no error")
	}

	// Scanning a column with the wrong type stops the reader.
	r = NewFormatReader(bytes.NewReader(typed), RowBinaryWithNamesAndTypes)
	var x uint64
	var mismatch TypeMismatchError
	if !r.Next() || !errors.As(Scan(r, UInt64, &x), &mismatch) || !errors.As(r.Err(), &mismatch) {
		t.Errorf("wrong scan type: err %v", r.Err())
	}
	if r.Next() {
		t.Error("reader continued after a type mismatch")
	}
}
