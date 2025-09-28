package rowbinary

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClient_Insert(t *testing.T) {
	assert := assert.New(t)
	tc := newTestCase()
	defer tc.Close()

	ctx := context.Background()
	c := NewClient(ctx, testClickHouseDSN, &ClientOptions{
		Database: tc.Database(),
	})

	assert.NoError(c.Exec(ctx, "CREATE TABLE t1 (x String) ENGINE = Memory"))

	assert.NoError(c.Insert(ctx, "t1", func(r *FormatWriter) error {
		for i := range 5 {
			if err := Write(r, String, fmt.Sprintf("%d", i)); err != nil {
				return err
			}
		}
		return nil
	}, NewColumn("x", String)))

	assert.ErrorContains(c.Insert(ctx, "t1", func(r *FormatWriter) error {
		for i := range 5 {
			if err := Write(r, String, fmt.Sprintf("%d", i)); err != nil {
				return err
			}
		}
		return fmt.Errorf("insertion failed")
	}, NewColumn("x", String)), "insertion failed")

}
