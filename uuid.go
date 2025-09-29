package rowbinary

import (
	"errors"

	"github.com/google/uuid"
)

var UUID Type[uuid.UUID] = typeUUID{}

type typeUUID struct{}

var typeUUIDID = BinaryTypeID(BinaryTypeUUID[:])

func (t typeUUID) String() string {
	return "UUID"
}

func (t typeUUID) Binary() []byte {
	return BinaryTypeUUID[:]
}

func (t typeUUID) ID() uint64 {
	return typeUUIDID
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

func (t typeUUID) WriteAny(w Writer, v any) error {
	value, ok := v.(uuid.UUID)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t typeUUID) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
