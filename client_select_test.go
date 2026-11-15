package rowbinary

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestClient_Select_InvalidDSN(t *testing.T) {
	assert := assert.New(t)
	ctx := context.Background()

	// Create client with invalid DSN
	c := NewClient(WithDSN("http://invalid-host:8123"))
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	err := c.Select(ctx,
		"SELECT 1 AS value",
		WithFormatReader(func(r *FormatReader) error {
			return nil
		}),
	)
	assert.Error(err)
	// Should contain connection error
	// assert.Contains(err.Error(), "context deadline exceeded") // or similar connection error
}
