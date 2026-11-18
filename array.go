package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

var _ Type[[]uint32] = Array(UInt32)

type typeArray[V any] struct {
	valueType Type[V]
	zeroWidth bool // elements take no bytes; see maxZeroWidthElements
}

func Array[V any](valueType Type[V]) Type[[]V] {
	return MakeTypeWrapAny(typeArray[V]{valueType: valueType, zeroWidth: isZeroWidthType(valueType)})
}

func (t typeArray[V]) String() string {
	return fmt.Sprintf("Array(%s)", t.valueType.String())
}

func (t typeArray[V]) Binary() []byte {
	return slices.Concat(BinaryTypeArray[:], t.valueType.Binary())
}

func (t typeArray[V]) Write(w Writer, value []V) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		err = t.valueType.Write(w, value[i])
		if err != nil {
			return err
		}
	}
	return err
}

func (t typeArray[V]) Scan(r Reader, v *[]V) error {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if n > uint64(^uint(0)>>1) {
		return fmt.Errorf("array length overflows int: %d", n)
	}
	if t.zeroWidth && n > 0 {
		if err := chargeZeroWidth(r, n, t.String()); err != nil {
			return err
		}
	}
	// Grow as elements are read, never by the declared count alone: a few
	// bytes can declare billions of elements. Existing capacity (and elements)
	// of the destination are reused.
	s := (*v)[:0]
	for i := uint64(0); i < n; i++ {
		if len(s) < cap(s) {
			s = s[:len(s)+1]
		} else {
			var zero V
			s = append(s, zero)
		}
		if err := t.valueType.Scan(r, &s[len(s)-1]); err != nil {
			*v = s
			return err
		}
	}
	*v = s
	return nil
}
