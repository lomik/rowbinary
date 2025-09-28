package rowbinary

import (
	"context"
	"net/http"
	"net/url"
)

const headerClickhouseQueryID = "X-ClickHouse-Query-Id"
const headerUserAgent = "User-Agent"
const httpUserAgent = "rowbinary/v0.1.0"

type ClientKind int

const (
	ClientKindSelect  ClientKind = 1
	ClientKindInsert  ClientKind = 2
	ClientKindExecute ClientKind = 3
)

type DiscoveryCtx struct {
	Kind ClientKind
}

// ClientOptions contains the options for creating a ClickHouse client.
type ClientOptions struct {
	HTTPClient *http.Client
	Discovery  func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
	Database   string
}

// Client represents a ClickHouse client.
type Client struct {
	dsn  string
	opts ClientOptions
}

// NewClient creates a new ClickHouse client.
func NewClient(ctx context.Context, dsn string, opts *ClientOptions) *Client {
	c := &Client{dsn: dsn}
	if opts != nil {
		c.opts = *opts
	}
	return c
}

func (c *Client) newRequest(ctx context.Context, discoCtx DiscoveryCtx, params url.Values) (*http.Request, error) {
	var err error
	dsn := c.dsn
	if c.opts.Discovery != nil {
		dsn, err = c.opts.Discovery(ctx, dsn, discoCtx)
		if err != nil {
			return nil, err
		}
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	url := &url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
	}
	values := url.Query()
	headers := http.Header{}
	headers.Set(headerUserAgent, httpUserAgent)

	if c.opts.Database != "" {
		values.Set("database", c.opts.Database)
	} else {
		values.Set("database", "default")
	}

	for k, v := range params {
		values.Set(k, v[0])
	}

	url.RawQuery = values.Encode()

	httpReq := (&http.Request{
		Method:     "POST",
		ProtoMajor: 1,
		ProtoMinor: 1,
		URL:        url,
		// TransferEncoding: []string{"chunked"},
		Header: headers,
	}).WithContext(ctx)

	if u.User.Username() != "" {
		password, _ := u.User.Password()
		httpReq.SetBasicAuth(u.User.Username(), password)
	}

	return httpReq, nil
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.opts.HTTPClient == nil {
		return http.DefaultClient.Do(req)
	}
	return c.opts.HTTPClient.Do(req)
}
