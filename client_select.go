package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strings"
)

type selectOptions struct {
	formatOptions   []FormatOption
	format          Format
	useBinaryHeader bool
	externalData    []externalData
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
		format:          RowBinaryWithNamesAndTypes,
		useBinaryHeader: true,
	}
	for _, opt := range options {
		opt.applySelectOptions(&opts)
	}

	params := url.Values{}
	if opts.useBinaryHeader {
		params.Set("output_format_binary_encode_types_in_binary_format", "1")
	}

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindSelect}, params)
	if err != nil {
		return err
	}
	req.Header.Set("X-ClickHouse-Format", opts.format.String())

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
