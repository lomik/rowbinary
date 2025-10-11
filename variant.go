package rowbinary

import (
	"encoding/binary"
	"fmt"
	"strings"
)

func Variant(valueTypes ...Any) Type[TypeValue] {
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

func (t typeVariant) Write(w Writer, value TypeValue) error {
	for i, tp := range t.valueTypes {
		if tp.id() == value.Type.id() {
			if err := VarintWrite(w, uint64(i)); err != nil {
				return err
			}

			return value.Type.WriteAny(w, value.Value)
		}
	}
	return TypeMismatchError{}
}

func (t typeVariant) Scan(r Reader, v *TypeValue) error {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if n >= uint64(len(t.valueTypes)) {
		return fmt.Errorf("invalid variant index: %d", n)
	}
	v.Type = t.valueTypes[n]
	return v.Type.ScanAny(r, &v.Value)
}
