package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

var _ Any = ArrayAny(UInt32)

type typeArrayAny struct {
	valueType Any
	zeroWidth bool // elements take no bytes; see maxZeroWidthElements
}

func ArrayAny(valueType Any) Type[[]any] {
	return MakeTypeWrapAny(typeArrayAny{valueType: valueType, zeroWidth: isZeroWidthType(valueType)})
}

func (t typeArrayAny) String() string {
	return fmt.Sprintf("Array(%s)", t.valueType.String())
}

func (t typeArrayAny) Binary() []byte {
	return slices.Concat(BinaryTypeArray[:], t.valueType.Binary())
}

func (t typeArrayAny) Write(w Writer, value []any) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	for i := 0; i < len(value); i++ {
		err = t.valueType.WriteAny(w, value[i])
		if err != nil {
			return err
		}
	}
	return err
}

func (t typeArrayAny) Scan(r Reader, v *[]any) error {
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
	// Grow as elements are read, never by the declared count alone.
	s := (*v)[:0]
	for i := uint64(0); i < n; i++ {
		var value any
		if err := t.valueType.ScanAny(r, &value); err != nil {
			*v = s
			return err
		}
		s = append(s, value)
	}
	*v = s
	return nil
}
