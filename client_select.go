package rowbinary

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type selectOptions struct {
	formatOptions []FormatOption
}

type SelectOption interface {
	applySelectOptions(*selectOptions)
}

func (c *Client) Select(ctx context.Context, query string, readFunc func(r *FormatReader) error, options ...SelectOption) error {
	opts := selectOptions{
		formatOptions: []FormatOption{
			RowBinaryWithNamesAndTypes,
			UseBinaryHeader(true),
		},
	}
	for _, opt := range options {
		opt.applySelectOptions(&opts)
	}

	req, err := c.newRequest(ctx, ClientKindSelect)
	if err != nil {
		return err
	}
	req.Body = io.NopCloser(strings.NewReader(query))

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return fmt.Errorf("unexpected status code: %d (%s)", resp.StatusCode, string(body))
	}

	if err := readFunc(NewFormatReader(resp.Body, opts.formatOptions...)); err != nil {
		return err
	}

	return nil
}
