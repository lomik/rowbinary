package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type insertOptions struct {
	formatOptions   []FormatOption
	format          Format
	useBinaryHeader bool
}

type InsertOption interface {
	applyInsertOptions(*insertOptions)
}

func (c *Client) Insert(ctx context.Context, table string, writeFunc func(w *FormatWriter) error, options ...InsertOption) error {
	opts := insertOptions{
		formatOptions: []FormatOption{
			RowBinaryWithNamesAndTypes,
			UseBinaryHeader(true),
		},
		format:          RowBinaryWithNamesAndTypes,
		useBinaryHeader: true,
	}
	for _, opt := range options {
		opt.applyInsertOptions(&opts)
	}

	params := url.Values{}
	if opts.useBinaryHeader {
		params.Set("input_format_binary_decode_types_in_binary_format", "1")
	}
	params.Set("query", fmt.Sprintf("INSERT INTO %s FORMAT %s", table, opts.format.String()))

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindInsert}, params)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	req.Body = r

	go func() {
		defer w.Close()
		bw := bufio.NewWriterSize(w, 1024*1024)
		defer bw.Flush()

		writer := NewFormatWriter(bw, opts.formatOptions...)
		if err := writer.WriteHeader(); err != nil {
			w.CloseWithError(err)
			return
		}

		if err := writeFunc(writer); err != nil {
			_ = w.CloseWithError(err)
		}
	}()

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
