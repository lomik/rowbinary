package rowbinary

import (
	"encoding/binary"
	"fmt"
	"slices"
)

var _ Any = ArrayAny(UInt32)

type typeArrayAny struct {
	valueType Any
}

func ArrayAny(valueType Any) Type[[]any] {
	return MakeTypeWrapAny(typeArrayAny{valueType: valueType})
}

func (t typeArrayAny) String() string {
	return fmt.Sprintf("Array(%s)", t.valueType.String())
}

func (t typeArrayAny) Binary() []byte {
	return slices.Concat(BinaryTypeArray[:], t.valueType.Binary())
}

func (t typeArrayAny) Write(w Writer, value []any) error {
	err := UVarint.Write(w, uint64(len(value)))
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
	if *v == nil {
		*v = make([]any, int(n))
	} else if len(*v) >= int(n) {
		*v = (*v)[:n]
	} else {
		*v = append(*v, make([]any, int(n)-len(*v))...)
	}

	for i := uint64(0); i < n; i++ {
		var value any
		err := t.valueType.ScanAny(r, &value)
		if err != nil {
			return err
		}
		(*v)[i] = value
		// if err := t.valueType.ScanAny(r, &(*v)[i]); err != nil {
		// 	return err
		// }
	}

	return nil
}
