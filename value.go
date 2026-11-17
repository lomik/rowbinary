package rowbinary

import (
	"errors"
	"fmt"
)

type Value struct {
	Type  Any
	Value any
}

var errValueWithoutType = errors.New("value has no type (use Value{Nothing, nil} for NULL)")

func (v Value) String() string {
	if v.Type == nil {
		return fmt.Sprintf("<nil>(%v)", v.Value)
	}
	return fmt.Sprintf("%s(%v)", v.Type.String(), v.Value)
}
