package rowbinary

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_Execute(t *testing.T) {
	assert := assert.New(t)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := NewClient(ctx, testClickHouseDSN, &ClientOptions{
		Database: tc.Database(),
	})

	err := c.Execute(ctx, "CREATE TABLE t1 (x String) ENGINE = Memory AS SELECT 1")
	assert.NoError(err)

	err = c.Execute(ctx, "CREATE TABLE")
	assert.ErrorContains(err, "Syntax error")
}
