package rowbinary

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type executeOptions struct {
}

type ExecuteOption interface {
	applyExecuteOptions(*executeOptions)
}

func (c *Client) Execute(ctx context.Context, query string, opts ...ExecuteOption) error {
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
