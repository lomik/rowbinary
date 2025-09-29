package rowbinary

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type execOptions struct {
}

type ExecuteOption interface {
	applyExecuteOptions(*execOptions)
}

func (c *client) Exec(ctx context.Context, query string, options ...ExecuteOption) error {
	opts := execOptions{}
	for _, opt := range options {
		opt.applyExecuteOptions(&opts)
	}

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindExecute}, url.Values{})
	if err != nil {
		return err
	}
	req.Body = io.NopCloser(strings.NewReader(query))

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d (%s)", resp.StatusCode, string(body))
	}

	return nil
}
