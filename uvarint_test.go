package rowbinary

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReader_Uvarint(t *testing.T) {
	t.Parallel()
	assert := assert.New(t)

	body, err := execLocal("SELECT 1 as c1, 2 as c2, 3 as c3, 4 as c4 FORMAT RowBinaryWithNamesAndTypes")
	assert.NoError(err)

	r := bytes.NewReader(body)

	value, err := UVarint.Read(r)
	assert.NoError(err)
	assert.Equal(uint64(4), value)
}
