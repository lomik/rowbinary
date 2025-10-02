package rowbinary

import (
	"context"
	"net/http"
	"net/url"
)

const headerClickhouseQueryID = "X-ClickHouse-Query-Id"
const headerUserAgent = "User-Agent"
const httpUserAgent = "rowbinary/v0.1.0"

type ClientMethod int

const (
	ClientMethodSelect  ClientMethod = 1
	ClientMethodInsert  ClientMethod = 2
	ClientMethodExecute ClientMethod = 3
)

type DiscoveryCtx struct {
	Method ClientMethod
}

type clientOptions struct {
	httpClient    *http.Client
	discovery     func(ctx context.Context, dsn string, kind DiscoveryCtx) (string, error)
	defaultSelect []SelectOption
	defaultInsert []InsertOption
	defaultExec   []ExecOption
}

type ClientOption interface {
	applyClientOptions(*clientOptions)
}

// Client defines the interface for interacting with a ClickHouse database server.
//
// It provides methods for executing SELECT queries to retrieve data, INSERT queries to add data,
// and general EXEC queries for DDL operations or updates. Implementations handle connection management,
// query execution, and data serialization in RowBinary format.
//
// The interface is designed to be used with context for cancellation and timeouts, and supports
// various options for customizing query behavior, such as format specifications, parameters, and headers.
type Client interface {
	// Select executes a SELECT query against the ClickHouse server and processes the response using the provided read function.
	//
	// It sends the query to ClickHouse, receives the result in RowBinary format (or variants), and invokes the readFunc
	// with a FormatReader to parse the data. The method supports various options for customizing the query format,
	// adding external data, parameters, and headers.
	//
	// Parameters:
	//   - ctx: Context for the request, used for cancellation and timeouts.
	//   - query: The SQL SELECT query string to execute.
	//   - readFunc: A callback function that receives a FormatReader to read and process the query results.
	//   - options: Optional SelectOption values to configure the query (e.g., format, external data, params).
	//
	// Returns:
	//   - error: An error if the request fails, the response status is not OK, or reading/parsing encounters issues.
	//
	// Note: The query results are streamed and processed incrementally via the readFunc. For external data,
	// the method uses multipart/form-data encoding. Default select options from the client configuration are applied first.
	Select(ctx context.Context, query string, readFunc func(r *FormatReader) error, options ...SelectOption) error

	// Exec executes an arbitrary SQL query (e.g., CREATE, DROP, ALTER) against the ClickHouse server.
	//
	// It sends the query as the request body and ensures the operation completes successfully.
	// This method is suitable for queries that do not return data, such as DDL statements or updates.
	//
	// Parameters:
	//   - ctx: Context for the request, used for cancellation and timeouts.
	//   - query: The SQL query string to execute.
	//   - options: Optional ExecOption values to configure the query (e.g., params, headers).
	//
	// Returns:
	//   - error: An error if the request fails or the response status is not OK.
	//
	// Note: Default exec options from the client configuration are applied first.
	// For queries that return data, use Select instead. For inserting data, use Insert.
	Exec(ctx context.Context, query string, options ...ExecOption) error

	// Insert executes an INSERT query into the specified ClickHouse table by writing data using the provided write function.
	//
	// It constructs an INSERT INTO query with the specified format, streams the data written by writeFunc to the server,
	// and ensures the operation completes successfully. The data is written incrementally via the FormatWriter.
	//
	// Parameters:
	//   - ctx: Context for the request, used for cancellation and timeouts.
	//   - table: The name of the ClickHouse table to insert data into.
	//   - writeFunc: A callback function that receives a FormatWriter to write the data to be inserted.
	//   - options: Optional InsertOption values to configure the insert (e.g., format, params, headers).
	//
	// Returns:
	//   - error: An error if the request fails, writing data encounters issues, or the response status is not OK.
	//
	// Note: The data is streamed to the server as it's written by writeFunc. Default insert options from the client
	// configuration are applied first. The format header is automatically written before invoking writeFunc.
	Insert(ctx context.Context, table string, writeFunc func(w *FormatWriter) error, options ...InsertOption) error

	Close() error
}

var _ ClientOption = WithUseBinaryHeader(false)
var _ ClientOption = RowBinary
var _ ClientOption = WithParam("key", "value")
var _ ClientOption = WithHeader("key", "value")
var _ ClientOption = WithDatabase("default")
var _ ClientOption = WithHTTPClient(nil)
var _ ClientOption = WithDiscovery(nil)

// Client represents a ClickHouse client.
type client struct {
	dsn  string
	opts clientOptions
}

// WithDatabase sets the database for the client.
func WithDatabase(database string) paramOption {
	return WithParam("database", database)
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
	// Apply default options
	WithUseBinaryHeader(true).applyClientOptions(&opts)
	RowBinaryWithNamesAndTypes.applyClientOptions(&opts)

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
