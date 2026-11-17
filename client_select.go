package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"slices"
	"strings"
)

type selectOptions struct {
	dsn           string
	formatOptions []FormatOption
	externalData  []externalData
	params        map[string]string
	headers       map[string]string
	formatReader  func(r *FormatReader) error
	bodyReader    func(r io.Reader) error
}

type SelectOption interface {
	applySelectOptions(*selectOptions)
}

/*
@TODO: handle response headers
HEADER: X-Clickhouse-Summary [{"read_rows":"588681","read_bytes":"4709448","written_rows":"0","written_bytes":"0","total_rows_to_read":"500000000","result_rows":"0","result_bytes":"0","elapsed_ns":"3477917"}]
HEADER: Date [Mon, 29 Sep 2025 15:15:30 GMT]
HEADER: Content-Type [application/octet-stream]
HEADER: Access-Control-Expose-Headers [X-ClickHouse-Query-Id,X-ClickHouse-Summary,X-ClickHouse-Server-Display-Name,X-ClickHouse-Format,X-ClickHouse-Timezone,X-ClickHouse-Exception-Code]
HEADER: X-Clickhouse-Query-Id [d9d2e809-284b-4502-ad4f-0cace04e9130]
HEADER: X-Clickhouse-Timezone [Europe/Moscow]
HEADER: Connection [Keep-Alive]
HEADER: X-Clickhouse-Server-Display-Name [mbp-rlomonosov-OZON-GJV47009WP]
HEADER: X-Clickhouse-Format [RowBinaryWithNamesAndTypes]
HEADER: Keep-Alive [timeout=30, max=9999]
*/

var _ SelectOption = C("", nil)
var _ SelectOption = WithUseBinaryHeader(false)
var _ SelectOption = RowBinary
var _ SelectOption = WithParam("key", "value")
var _ SelectOption = WithHeader("key", "value")
var _ SelectOption = WithExternalData("key")
var _ SelectOption = WithFormatReader(nil)
var _ SelectOption = WithBodyReader(nil)
var _ SelectOption = WithDSN("http://localhost:8123")

func (c *client) Select(ctx context.Context, query string, options ...SelectOption) error {
	opts := selectOptions{
		params:  map[string]string{},
		headers: map[string]string{},
	}
	for _, opt := range c.opts.defaultSelect {
		opt.applySelectOptions(&opts)
	}
	for _, opt := range options {
		if opt == nil {
			continue
		}
		opt.applySelectOptions(&opts)
	}
	for _, ed := range opts.externalData {
		if ed.cb == nil {
			return fmt.Errorf("external data %q requires a format writer", ed.name)
		}
	}

	req, err := c.newRequest(ctx, opts.dsn, DiscoveryCtx{Method: ClientMethodSelect}, opts.params, opts.headers)
	if err != nil {
		return err
	}

	// should attach files
	if len(opts.externalData) > 0 {
		tmpWriter := multipart.NewWriter(io.Discard)
		req.Header.Set("Content-Type", tmpWriter.FormDataContentType())

		r, w := io.Pipe()
		req.Body = r

		go func() {
			// Every exit closes the pipe exactly once: with the error on failure, or
			// cleanly after the closing boundary has been written and flushed.
			bw := bufio.NewWriterSize(w, 1024*1024)
			mw := multipart.NewWriter(bw)
			mw.SetBoundary(tmpWriter.Boundary())

			if err := mw.WriteField("query", query); err != nil {
				w.CloseWithError(err)
				return
			}

			for _, ed := range opts.externalData {
				fw := NewFormatWriter(io.Discard, append(slices.Clone(ed.formatOptions), RowBinary)...)

				if err := mw.WriteField(ed.name+"_structure", fw.Structure()); err != nil {
					w.CloseWithError(err)
					return
				}

				if err := mw.WriteField(ed.name+"_format", fw.Format().String()); err != nil {
					w.CloseWithError(err)
					return
				}
			}

			for _, ed := range opts.externalData {
				ff, err := mw.CreateFormFile(ed.name, ed.name)
				if err != nil {
					w.CloseWithError(err)
					return
				}

				fw := NewFormatWriter(ff, append(slices.Clone(ed.formatOptions), RowBinary)...)

				if err := ed.cb(fw); err != nil {
					w.CloseWithError(err)
					return
				}
			}

			if err := mw.Close(); err != nil {
				w.CloseWithError(err)
				return
			}
			w.CloseWithError(bw.Flush()) // nil error closes the pipe normally
		}()
	} else {
		req.Body = io.NopCloser(strings.NewReader(query))
	}

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}

	// px := &ReadSizeProxy{Reader: resp.Body}
	// defer px.Close()

	body := &tailReader{r: resp.Body}

	if opts.formatReader != nil {
		fr := NewFormatReader(body, opts.formatOptions...)
		if err := opts.formatReader(fr); err != nil {
			return err
		}
		// The callback may stop at the first failed Next/Scan and return nil;
		// a cut-off or failed stream must still be reported.
		if err := fr.Err(); err != nil && err != io.EOF {
			return selectStreamError(err, body)
		}
		return nil
	}

	if opts.bodyReader != nil {
		if err := opts.bodyReader(resp.Body); err != nil {
			return err
		}
		return nil
	}

	// No reader: consume the response so that a truncated stream or a query
	// that failed after the headers were sent is reported, not ignored.
	if _, err := io.Copy(io.Discard, body); err != nil {
		return selectStreamError(err, body)
	}
	return nil
}

func WithFormatReader(cb func(r *FormatReader) error) SelectOption {
	return formatReaderOption{cb: cb}
}

type formatReaderOption struct {
	cb func(r *FormatReader) error
}

func (f formatReaderOption) applySelectOptions(opts *selectOptions) {
	opts.formatReader = f.cb
}

func WithBodyReader(cb func(r io.Reader) error) SelectOption {
	return bodyReaderOption{cb: cb}
}

type bodyReaderOption struct {
	cb func(r io.Reader) error
}

func (b bodyReaderOption) applySelectOptions(opts *selectOptions) {
	opts.bodyReader = b.cb
}
