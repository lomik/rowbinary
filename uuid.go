package rowbinary

import (
	"github.com/google/uuid"
)

var UUID Type[uuid.UUID] = MakeTypeWrapAny[uuid.UUID](typeUUID{})

type typeUUID struct{}

func (t typeUUID) String() string {
	return "UUID"
}

func (t typeUUID) Binary() []byte {
	return BinaryTypeUUID[:]
}

func (t typeUUID) Write(w Writer, value uuid.UUID) error {
	tmp, err := value.MarshalBinary()
	if err != nil {
		return err
	}
	buf := w.Buffer()
	copy(buf, tmp)
	swap64(buf)
	_, err = w.Write(buf)

	return err
}

func (t typeUUID) Scan(r Reader, v *uuid.UUID) error {
	b, err := r.Peek(16)
	if err != nil {
		return err
	}

	swap64(b)
	err = v.UnmarshalBinary(b)
	if err != nil {
		return err
	}

	_, err = r.Discard(16)
	return err
}
