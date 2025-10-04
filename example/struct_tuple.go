package example

import (
	"github.com/lomik/rowbinary"
)

type StructTuple struct {
	ID   uint32
	Name []byte
}

var structTupleTypeOrigin = rowbinary.TupleNamedAny(
	rowbinary.C("id", rowbinary.UInt32),
	rowbinary.C("name", rowbinary.StringBytes),
)

var StructTupleType rowbinary.Type[StructTuple] = rowbinary.MakeTypeWrapAny[StructTuple](structTupleType{})

type structTupleType struct{}

func (t structTupleType) String() string {
	return structTupleTypeOrigin.String()
}

func (t structTupleType) Binary() []byte {
	return structTupleTypeOrigin.Binary()
}

func (t structTupleType) Write(w rowbinary.Writer, v StructTuple) error {
	if err := rowbinary.UInt32.Write(w, v.ID); err != nil {
		return err
	}
	if err := rowbinary.StringBytes.Write(w, v.Name); err != nil {
		return err
	}
	return nil
}

func (t structTupleType) Scan(r rowbinary.Reader, v *StructTuple) error {
	if err := rowbinary.UInt32.Scan(r, &v.ID); err != nil {
		return err
	}
	if err := rowbinary.StringBytes.Scan(r, &v.Name); err != nil {
		return err
	}
	return nil
}
