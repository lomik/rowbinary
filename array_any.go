package rowbinary

import (
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

func (t typeArrayAny) Read(r Reader) ([]any, error) {
	n, err := UVarint.Read(r)
	if err != nil {
		return nil, err
	}

	ret := make([]any, 0, int(n))
	for i := uint64(0); i < n; i++ {
		s, err := t.valueType.ReadAny(r)
		if err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}

	return ret, nil
}
