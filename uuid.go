package rowbinary

import (
	"io"

	"github.com/google/uuid"
	"github.com/pkg/errors"
)

var UUID Type[uuid.UUID] = &typeUUID{}

type typeUUID struct {
}

func (t *typeUUID) String() string {
	return "UUID"
}

func (t *typeUUID) Binary() []byte {
	return BinaryTypeUUID[:]
}

func (t *typeUUID) Write(w Writer, value uuid.UUID) error {
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

func (t *typeUUID) Read(r Reader) (uuid.UUID, error) {
	_, err := io.ReadAtLeast(r, r.buffer()[:16], 16)
	if err != nil {
		return uuid.UUID{}, err
	}

	swap64(r.buffer()[:16])

	return uuid.FromBytes(r.buffer()[:16])
}

func (t *typeUUID) WriteAny(w Writer, v any) error {
	value, ok := v.(uuid.UUID)
	if !ok {
		return errors.New("unexpected type")
	}
	return t.Write(w, value)
}

func (t *typeUUID) ReadAny(r Reader) (any, error) {
	return t.Read(r)
}
