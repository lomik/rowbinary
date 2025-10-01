package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
)

type selectOptions struct {
	formatOptions []FormatOption
	externalData  []externalData
	params        map[string]string
	headers       map[string]string
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
var _ SelectOption = WithExternalData("key", func(w *FormatWriter) error { return nil })

func (c *client) Select(ctx context.Context, query string, readFunc func(r *FormatReader) error, options ...SelectOption) error {
	opts := selectOptions{
		params:  map[string]string{},
		headers: map[string]string{},
	}
	for _, opt := range c.opts.defaultSelect {
		opt.applySelectOptions(&opts)
	}
	for _, opt := range options {
		opt.applySelectOptions(&opts)
	}

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindSelect}, opts.params, opts.headers)
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
			defer w.Close()
			bw := bufio.NewWriterSize(w, 1024*1024)
			defer bw.Flush()
			mw := multipart.NewWriter(bw)
			mw.SetBoundary(tmpWriter.Boundary())
			defer mw.Close()

			if err := mw.WriteField("query", query); err != nil {
				w.CloseWithError(err)
				return
			}

			for _, ed := range opts.externalData {
				fw := NewFormatWriter(io.Discard, append(ed.formatOptions, RowBinary)...)

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

				fw := NewFormatWriter(ff, append(ed.formatOptions, RowBinary)...)

				if err := ed.cb(fw); err != nil {
					w.CloseWithError(err)
					return
				}
			}

			if err := mw.Close(); err != nil {
				w.CloseWithError(err)
				return
			}
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
