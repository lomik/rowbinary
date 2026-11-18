package rowbinary

import (
	"fmt"
	"slices"
)

// DynamicDefaultMaxTypes is the max_types ClickHouse uses for a plain `Dynamic` column.
const DynamicDefaultMaxTypes uint8 = 32

// DynamicDefault is the type of a plain `Dynamic` column, i.e. Dynamic(max_types=32).
var DynamicDefault = Dynamic(DynamicDefaultMaxTypes)

// Dynamic returns the ClickHouse type Dynamic(max_types=maxTypes). As in ClickHouse,
// maxTypes 0 is a valid limit (every value is stored in the shared variant), not
// "use the default": pass DynamicDefaultMaxTypes, or use DynamicDefault, for a plain
// `Dynamic` column. knownTypes are type instances to use when a scanned value has
// the same type, for example a custom Go type for a tuple.
func Dynamic(maxTypes uint8, knownTypes ...Any) Type[Value] {
	return MakeTypeWrapAny(typeDynamic{
		maxTypes:   maxTypes,
		knownTypes: slices.Clone(knownTypes),
	})
}

type typeDynamic struct {
	maxTypes   uint8
	knownTypes []Any
}

func (t typeDynamic) String() string {
	if t.maxTypes == DynamicDefaultMaxTypes {
		return "Dynamic"
	}
	return fmt.Sprintf("Dynamic(max_types=%d)", t.maxTypes)
}

func (t typeDynamic) Binary() []byte {
	return append(BinaryTypeDynamic[:], t.maxTypes)
}

func (t typeDynamic) Write(w Writer, value Value) error {
	if value.Type == nil {
		return errValueWithoutType
	}
	_, err := w.Write(typeBinary(value.Type))
	if err != nil {
		return err
	}
	return value.Type.WriteAny(w, value.Value)
}

func (t typeDynamic) Scan(r Reader, v *Value) error {
	tp, err := DecodeBinaryType(r)
	if err != nil {
		return err
	}
	v.Type = tp
	for _, k := range t.knownTypes {
		if k.ID() == tp.ID() {
			v.Type = k
			break
		}
	}
	return v.Type.ScanAny(r, &v.Value)
}
