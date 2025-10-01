package example

import (
	"github.com/lomik/rowbinary"
)

type CustomTuple struct {
	ID   uint32
	Name string
}

var CustomTupleType rowbinary.Type[CustomTuple] = rowbinary.MakeTypeWrapAny[CustomTuple](customTupleType{})

var customTupleTypeOrigin = rowbinary.TupleNamedAny(
	rowbinary.C("id", rowbinary.UInt32),
	rowbinary.C("name", rowbinary.String),
)

type customTupleType struct{}

func (t customTupleType) String() string {
	return customTupleTypeOrigin.String()
}

func (t customTupleType) Binary() []byte {
	return customTupleTypeOrigin.Binary()
}

func (t customTupleType) Read(r rowbinary.Reader) (CustomTuple, error) {
	id, err := rowbinary.UInt32.Read(r)
	if err != nil {
		return CustomTuple{}, err
	}
	name, err := rowbinary.String.Read(r)
	if err != nil {
		return CustomTuple{}, err
	}
	return CustomTuple{
		ID:   id,
		Name: name,
	}, nil
}

func (t customTupleType) Write(w rowbinary.Writer, v CustomTuple) error {
	if err := rowbinary.UInt32.Write(w, v.ID); err != nil {
		return err
	}
	if err := rowbinary.String.Write(w, v.Name); err != nil {
		return err
	}
	return nil
}
