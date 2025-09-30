package rowbinary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_Exec(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()
	c := NewTestClient(ctx, testClickHouseDSN)
	defer c.Close()

	err := c.Exec(ctx, "CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1")
	assert.NoError(err)

	err = c.Exec(ctx, "CREATE TABLE")
	assert.ErrorContains(err, "Syntax error")
}
