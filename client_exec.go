package rowbinary

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type execOptions struct {
	params  map[string]string
	headers map[string]string
}

type ExecOption interface {
	applyExecOptions(*execOptions)
}

var _ ExecOption = WithParam("key", "value")
var _ ExecOption = WithHeader("key", "value")

func (c *client) Exec(ctx context.Context, query string, options ...ExecOption) error {
	opts := execOptions{
		params:  map[string]string{},
		headers: map[string]string{},
	}
	for _, opt := range c.opts.defaultExec {
		opt.applyExecOptions(&opts)
	}
	for _, opt := range options {
		opt.applyExecOptions(&opts)
	}

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindExecute}, opts.params, opts.headers)
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
