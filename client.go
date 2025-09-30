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

type clientOptions struct {
	httpClient *http.Client
	discovery  func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
	database   string
}

type ClientOption interface {
	applyClientOptions(*clientOptions)
}

type Client interface {
	Select(ctx context.Context, query string, readFunc func(r *FormatReader) error, options ...SelectOption) error
	Exec(ctx context.Context, query string, options ...ExecOption) error
	Insert(ctx context.Context, table string, writeFunc func(w *FormatWriter) error, options ...InsertOption) error
	Close() error
}

// Client represents a ClickHouse client.
type client struct {
	dsn  string
	opts clientOptions
}

type clientOptionDatabase struct {
	database string
}

func (o clientOptionDatabase) applyClientOptions(opts *clientOptions) {
	opts.database = o.database
}

// WithDatabase sets the database for the client.
func WithDatabase(database string) ClientOption {
	return clientOptionDatabase{database: database}
}

type clientOptionHTTPClient struct {
	httpClient *http.Client
}

func (o clientOptionHTTPClient) applyClientOptions(opts *clientOptions) {
	opts.httpClient = o.httpClient
}

// WithHTTPClient sets the HTTP client for the ClickHouse client.
func WithHTTPClient(httpClient *http.Client) ClientOption {
	return clientOptionHTTPClient{httpClient: httpClient}
}

type clientOptionDiscovery struct {
	discovery func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
}

func (o clientOptionDiscovery) applyClientOptions(opts *clientOptions) {
	opts.discovery = o.discovery
}

// WithDiscovery sets the discovery function for the ClickHouse client.
func WithDiscovery(discovery func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)) ClientOption {
	return clientOptionDiscovery{discovery: discovery}
}

// NewClient creates a new ClickHouse client.
func NewClient(ctx context.Context, dsn string, options ...ClientOption) Client {
	opts := clientOptions{}
	for _, opt := range options {
		if opt != nil {
			opt.applyClientOptions(&opts)
		}
	}

	c := &client{dsn: dsn, opts: opts}

	if c.opts.httpClient == nil {
		c.opts.httpClient = &http.Client{
			Transport: &http.Transport{
				ReadBufferSize:  1024 * 1024,
				WriteBufferSize: 1024 * 1024,
			},
		}
	}
	return c
}

func (c *client) newRequest(ctx context.Context, discoCtx DiscoveryCtx, pp map[string]string, hh map[string]string) (*http.Request, error) {
	var err error
	dsn := c.dsn
	if c.opts.discovery != nil {
		dsn, err = c.opts.discovery(ctx, dsn, discoCtx)
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

	if c.opts.database != "" {
		values.Set("database", c.opts.database)
	} else {
		values.Set("database", "default")
	}

	for k, v := range pp {
		values.Set(k, v)
	}
	for k, v := range hh {
		headers.Set(k, v)
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

func (c *client) do(req *http.Request) (*http.Response, error) {
	return c.opts.httpClient.Do(req)
}

func (c *client) Close() error {
	return nil
}
