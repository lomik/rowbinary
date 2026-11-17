package rowbinary

import (
	"context"
	"testing"
	"time"
)

func TestClient_Select_InvalidDSN(t *testing.T) {
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
	if err == nil {
		t.Fatal("Select against an unreachable host returned nil")
	}
}
