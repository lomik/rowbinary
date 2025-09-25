package client

import (
	"context"

	"github.com/valyala/fasthttp"
)

// Options contains the options for creating a ClickHouse client.
type Options struct {
	HTTPClient *fasthttp.Client
}

// Client represents a ClickHouse client.
type Client struct {
	dsn  string
	opts Options
}

// New creates a new ClickHouse client.
func New(ctx context.Context, dsn string, opts *Options) *Client {
	c := &Client{dsn: dsn}
	if opts != nil {
		c.opts = *opts
	}
	return c
}
