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
	buf := w.buffer()
	copy(buf, tmp)
	swap64(buf)
	_, err = w.Write(buf)

	return err
}

func (t typeUUID) Read(r Reader) (uuid.UUID, error) {
	b, err := r.Peek(16)
	if err != nil {
		return uuid.UUID{}, err
	}

	swap64(b)
	ret, err := uuid.FromBytes(b)
	r.Discard(16)

	return ret, err
}

func (t typeUUID) Scan(r Reader, v *uuid.UUID) (err error) {
	*v, err = t.Read(r)
	return
}
