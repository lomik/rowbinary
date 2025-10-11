package rowbinary

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
)

func VariantAny(valueTypes ...Any) Type[any] {
	t := make([]Any, len(valueTypes))
	copy(t, valueTypes)
	sort.Slice(t, func(i, j int) bool {
		return t[i].String() < t[j].String()
	})
	return MakeTypeWrapAny(typeVariantAny{
		valueTypes: t,
	})
}

type typeVariantAny struct {
	valueTypes []Any
}

func (t typeVariantAny) String() string {
	var types []string
	for _, vt := range t.valueTypes {
		types = append(types, vt.String())
	}
	return fmt.Sprintf("Variant(%s)", strings.Join(types, ", "))
}

func (t typeVariantAny) Binary() []byte {
	tbin := append(BinaryTypeVariant[:], VarintEncode(uint64(len(t.valueTypes)))...)
	for _, vt := range t.valueTypes {
		tbin = append(tbin, vt.Binary()...)
	}
	return tbin
}

func (t typeVariantAny) Write(w Writer, value any) error {
	var buf bytes.Buffer
	wbuf := NewWriter(&buf)
	var err error
	for i, tp := range t.valueTypes {
		buf.Reset()
		err = tp.WriteAny(wbuf, value)
		if err != nil {
			if errors.Is(err, TypeMismatchError{}) {
				continue
			}
			return err
		}
		err = VarintWrite(w, uint64(i))
		if err != nil {
			return err
		}

		_, err = buf.WriteTo(w)
		if err != nil {
			return err
		}

		return nil
	}
	return TypeMismatchError{}
}

func (t typeVariantAny) Scan(r Reader, v *any) error {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	return t.valueTypes[n].ScanAny(r, v)
}
