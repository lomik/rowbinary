package client

import (
	"context"
	"testing"

	"github.com/pluto-metrics/rowbinary"
	"github.com/pluto-metrics/rowbinary/schema"
	"github.com/stretchr/testify/assert"
)

func TestSelectOne(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	c := New(ctx, "http://user:password@127.0.0.1:8123", nil)

	err := c.Select(ctx, "SELECT toInt32(1)", nil, func(r *schema.Reader) error {
		v, err := schema.Read(r, rowbinary.Int32)
		assert.NoError(err)
		assert.Equal(int32(1), v)
		return nil
	})

	assert.NoError(err)
}
