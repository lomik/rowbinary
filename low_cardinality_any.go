package rowbinary

import (
	"fmt"
)

func LowCardinalityAny(valueType Any) Type[any] {
	return MakeTypeWrapAny(typeLowCardinalityAny{
		valueType: valueType,
	})
}

type typeLowCardinalityAny struct {
	id        uint64
	valueType Any
	tbin      []byte
	tstr      string
}

func (t typeLowCardinalityAny) String() string {
	return fmt.Sprintf("LowCardinality(%s)", t.valueType.String())
}

func (t typeLowCardinalityAny) Binary() []byte {
	return append(BinaryTypeLowCardinality[:], t.valueType.Binary()...)
}

func (t typeLowCardinalityAny) Write(w Writer, value any) error {
	return t.valueType.WriteAny(w, value)
}

func (t typeLowCardinalityAny) Scan(r Reader, v *any) (err error) {
	return t.valueType.ScanAny(r, v)
}
