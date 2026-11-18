package rowbinary

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// maxZeroWidthElements limits arrays (and maps) whose elements take no bytes,
// such as Array(Nothing) or Array(Tuple()). Their declared length cannot be
// checked against the input, so a few bytes could otherwise declare 2^40
// elements and make the reader loop and allocate without bound. Real data
// essentially never has such arrays (Array(Nothing) values are always empty).
//
// FormatReader applies it as a budget per row, across all nesting levels;
// codecs used directly with another Reader apply it per array.
const maxZeroWidthElements = 1 << 20

// isZeroWidthType reports whether every value of tp is encoded in zero bytes:
// Nothing, Tuple() and tuples or LowCardinality of such types.
func isZeroWidthType(tp Any) bool {
	return zeroWidthEncoding(bytes.NewReader(typeBinary(tp)))
}

func zeroWidthEncoding(r *bytes.Reader) bool {
	tag, err := r.ReadByte()
	if err != nil {
		return false
	}
	switch tag {
	case BinaryTypeNothing[0]:
		return true
	case BinaryTypeLowCardinality[0]:
		return zeroWidthEncoding(r)
	case BinaryTypeTuple[0]:
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return false
		}
		for range n {
			if !zeroWidthEncoding(r) {
				return false
			}
		}
		return true
	case BinaryTypeTupleNamed[0]:
		n, err := binary.ReadUvarint(r)
		if err != nil {
			return false
		}
		for range n {
			nameLen, err := binary.ReadUvarint(r)
			if err != nil {
				return false
			}
			if _, err := r.Seek(int64(nameLen), io.SeekCurrent); err != nil {
				return false
			}
			if !zeroWidthEncoding(r) {
				return false
			}
		}
		return true
	}
	return false
}

// budgetReader is the Reader of a FormatReader: it carries the zero-width
// element budget of the current row.
type budgetReader struct {
	Reader
	zeroWidthLeft uint64
}

func (b *budgetReader) resetRow() {
	b.zeroWidthLeft = maxZeroWidthElements
}

// chargeZeroWidth accounts for n zero-width elements of a collection of type
// what, failing once the row budget (or, without a FormatReader, the
// per-collection limit) is exceeded.
func chargeZeroWidth(r Reader, n uint64, what string) error {
	if b, ok := r.(*budgetReader); ok {
		if n > b.zeroWidthLeft {
			return fmt.Errorf("%s: %d elements of zero width exceed the limit of %d per row", what, n, maxZeroWidthElements)
		}
		b.zeroWidthLeft -= n
		return nil
	}
	if n > maxZeroWidthElements {
		return fmt.Errorf("%s: %d elements of zero width exceed the limit of %d", what, n, maxZeroWidthElements)
	}
	return nil
}
