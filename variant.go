package rowbinary

import (
	"fmt"
	"slices"
	"strings"
)

func Variant(valueTypes ...Any) Type[Value] {
	valueTypes = slices.Clone(valueTypes)
	slices.SortFunc(valueTypes, func(a, b Any) int {
		return strings.Compare(a.String(), b.String())
	})
	return MakeTypeWrapAny(typeVariant{
		valueTypes: valueTypes,
	})
}

type typeVariant struct {
	valueTypes []Any
}

func (t typeVariant) String() string {
	var types []string
	for _, vt := range t.valueTypes {
		types = append(types, vt.String())
	}
	return fmt.Sprintf("Variant(%s)", strings.Join(types, ", "))
}

func (t typeVariant) Binary() []byte {
	tbin := append(BinaryTypeVariant[:], VarintEncode(uint64(len(t.valueTypes)))...)
	for _, vt := range t.valueTypes {
		tbin = append(tbin, vt.Binary()...)
	}
	return tbin
}

func (t typeVariant) Write(w Writer, value Value) error {
	if value.Type != nil && Eq(value.Type, Nothing) && value.Value == nil {
		return w.WriteByte(255)
	}
	for i, tp := range t.valueTypes {
		if tp.ID() == value.Type.ID() {
			if i >= 255 {
				return fmt.Errorf("invalid variant index: %d", i)
			}
			if err := w.WriteByte(byte(i)); err != nil {
				return err
			}

			return value.Type.WriteAny(w, value.Value)
		}
	}
	return TypeMismatchError{}
}

func (t typeVariant) Scan(r Reader, v *Value) error {
	n, err := r.ReadByte()
	if err != nil {
		return err
	}
	if n == 255 {
		*v = Value{Nothing, nil}
		return nil
	}
	if int(n) >= len(t.valueTypes) {
		return fmt.Errorf("invalid variant index: %d", n)
	}
	v.Type = t.valueTypes[n]
	return v.Type.ScanAny(r, &v.Value)
}
