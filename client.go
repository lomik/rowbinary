package rowbinary

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
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
	Select(ctx context.Context, query string, options ...SelectOption) error

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

	// Insert sends `INSERT INTO <table> FORMAT <format>` and streams the rows
	// written by the WithFormatWriter (or WithBodyWriter) callback as the body.
	// One of the two writers is required. The format defaults to
	// RowBinaryWithNamesAndTypes; client defaults are applied before options.
	//
	// table is inserted into the SQL as written, like the query of Select and
	// Exec: it may be `db.table`, include a column list or settings, and must not
	// come from untrusted input unquoted (see QuoteIdentifier).
	//
	// Column mapping depends on the format:
	//   - RowBinaryWithNames and RowBinaryWithNamesAndTypes (the default) send
	//     the column names in a header, and ClickHouse matches them by name;
	//     table columns that are not sent get their defaults.
	//   - RowBinary has no header: ClickHouse assigns values to the table's
	//     columns by position, in table order. The column options are used only
	//     to type-check the written values, not to name columns. To write in a
	//     different order or a subset of columns, put the list in table, for
	//     example "events (ts, id)", or use a format with names.
	//
	// Insert returns the first error of the request, the writer callback, or a
	// non-200 response.
	//
	// The writer callback runs on its own goroutine, feeding the upload. When
	// ctx is done, Insert returns ctx's error without waiting for it, and the
	// callback's next write fails because the upload is closed. A callback that
	// waits on anything else (a channel, a slow source) should watch the
	// context and return: use WithFormatWriterContext or WithBodyWriterContext
	// to receive it, or close over the ctx passed to Insert.
	Insert(ctx context.Context, table string, options ...InsertOption) error

	// Close releases resources owned by the client. A client owns none: its
	// connections belong either to the package-wide default pool (shared by
	// all clients without their own HTTP client, which closes idle connections
	// after 90 seconds) or to the http.Client given with WithHTTPClient or
	// SetGlobalHTTPClient, which stays the caller's to manage, e.g. with
	// http.Client.CloseIdleConnections. So Close does not touch connections,
	// always returns nil, and the client remains usable afterwards.
	Close() error
}

var _ ClientOption = WithUseBinaryHeader(false)
var _ ClientOption = RowBinary
var _ ClientOption = WithParam("key", "value")
var _ ClientOption = WithHeader("key", "value")
var _ ClientOption = WithDatabase("default")
var _ ClientOption = WithHTTPClient(nil)
var _ ClientOption = WithDiscovery(nil)
var _ ClientOption = WithDSN("http://localhost:8123")

// Client represents a ClickHouse client.
type client struct {
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
func NewClient(options ...ClientOption) Client {
	opts := clientOptions{}
	// Apply default options
	WithUseBinaryHeader(false).applyClientOptions(&opts)
	RowBinaryWithNamesAndTypes.applyClientOptions(&opts)

	for _, opt := range options {
		if opt != nil {
			opt.applyClientOptions(&opts)
		}
	}

	c := &client{opts: opts}

	if c.opts.httpClient == nil {
		c.opts.httpClient = getGlobalHTTPClient()
	}

	if c.opts.httpClient == nil {
		c.opts.httpClient = defaultHTTPClient()
	}
	return c
}

func (c *client) newRequest(ctx context.Context, dsn string, discoCtx DiscoveryCtx, pp map[string]string, hh map[string]string) (*http.Request, error) {
	var err error
	if c.opts.discovery != nil {
		dsn, err = c.opts.discovery(ctx, dsn, discoCtx)
		if err != nil {
			return nil, err
		}
	} else {
		cb := getGlobalDiscovery()
		if cb != nil {
			dsn, err = cb(ctx, dsn, discoCtx)
			if err != nil {
				return nil, err
			}
		}
	}

	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	url := &url.URL{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Path:     u.Path,
		RawPath:  u.RawPath,
		RawQuery: u.RawQuery,
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
	resp, err := c.opts.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	// On 301/302/303 the HTTP client follows the redirect with GET and no
	// body: the query and data are dropped, and a 200 from the redirect target
	// would look like success. Treat a changed method as an error.
	if final := resp.Request; final != nil && final.Method != req.Method {
		resp.Body.Close()
		return nil, fmt.Errorf("server redirected the %s request to %s and the HTTP client re-sent it as %s without the query and data; put the final URL in the DSN", req.Method, redirectTarget(final.URL), final.Method)
	}
	return resp, nil
}

// redirectTarget is u without query parameters (which may hold settings).
func redirectTarget(u *url.URL) string {
	v := *u
	v.RawQuery = ""
	v.ForceQuery = false
	v.User = nil
	return v.String()
}

// setReplayableBody sets an in-memory request body that the HTTP client can
// re-send when it follows a 307 or 308 redirect.
func setReplayableBody(req *http.Request, body string) {
	req.Body = io.NopCloser(strings.NewReader(body))
	req.ContentLength = int64(len(body))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(strings.NewReader(body)), nil
	}
}

// Close is a no-op by design; see Client.Close.
func (c *client) Close() error {
	return nil
}
