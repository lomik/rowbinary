package client

import (
	"context"
	"fmt"

	"github.com/pluto-metrics/rowbinary/schema"
	"github.com/valyala/fasthttp"
)

type SelectOptions struct {
}

func (c *Client) Select(ctx context.Context, query string, opts *SelectOptions, fn func(r *schema.Reader) error) error {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)

	err := c.prepare(ctx, req)
	if err != nil {
		return err
	}

	req.SetBody([]byte(query))
	resp.StreamBody = true

	err = c.do(req, resp)
	if err != nil {
		return err
	}

	if resp.StatusCode() != fasthttp.StatusOK {
		return fmt.Errorf("unexpected status code: %d (%s)", resp.StatusCode(), string(resp.Body()))
	}

	reader := schema.NewReader(
		resp.BodyStream(),
		schema.Format(schema.RowBinaryWithNamesAndTypes),
		schema.Binary(true),
	)

	err = reader.ReadHeader()
	if err != nil {
		return err
	}

	err = fn(reader)
	if err != nil {
		return err
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	return nil
}
