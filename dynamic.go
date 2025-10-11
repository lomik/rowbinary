package rowbinary

import (
	"fmt"
)

func DynamicAny(maxTypes uint8, knownTypes ...Any) Type[TypeValue] {
	if maxTypes == 0 {
		maxTypes = 32
	}
	return MakeTypeWrapAny(typeDynamicAny{
		maxTypes:   maxTypes,
		knownTypes: knownTypes,
	})
}

type typeDynamicAny struct {
	maxTypes   uint8
	knownTypes []Any
}

func (t typeDynamicAny) String() string {
	if t.maxTypes == 32 {
		return "Dynamic"
	}
	return fmt.Sprintf("Dynamic(max_types=%d)", t.maxTypes)
}

func (t typeDynamicAny) Binary() []byte {
	return append(BinaryTypeDynamic[:], t.maxTypes)
}

func (t typeDynamicAny) Write(w Writer, value TypeValue) error {
	_, err := w.Write(value.Type.Binary())
	if err != nil {
		return err
	}
	return value.Type.WriteAny(w, value.Value)
}

func (t typeDynamicAny) Scan(r Reader, v *TypeValue) error {
	tp, err := DecodeBinaryType(r)
	if err != nil {
		return err
	}
	v.Type = tp
	for _, k := range t.knownTypes {
		if k.id() == tp.id() {
			v.Type = k
			break
		}
	}
	return v.Type.ScanAny(r, &v.Value)
}
