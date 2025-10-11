package rowbinary

import "fmt"

type TypeValue struct {
	Type  Any
	Value any
}

func (v TypeValue) String() string {
	return fmt.Sprintf("%s(%v)", v.Type.String(), v.Value)
}
