package rowbinary

import "fmt"

type Value struct {
	Type  Any
	Value any
}

func (v Value) String() string {
	return fmt.Sprintf("%s(%v)", v.Type.String(), v.Value)
}
