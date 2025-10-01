package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
)

type insertOptions struct {
	formatOptions []FormatOption
	format        Format
	params        map[string]string
	headers       map[string]string
}

type InsertOption interface {
	applyInsertOptions(*insertOptions)
}

var _ InsertOption = C("", nil)
var _ InsertOption = WithUseBinaryHeader(false)
var _ InsertOption = RowBinary
var _ InsertOption = WithParam("key", "value")
var _ InsertOption = WithHeader("key", "value")

func (c *client) Insert(ctx context.Context, table string, writeFunc func(w *FormatWriter) error, options ...InsertOption) error {
	opts := insertOptions{
		params:  map[string]string{},
		headers: map[string]string{},
	}

	for _, opt := range c.opts.defaultInsert {
		opt.applyInsertOptions(&opts)
	}
	for _, opt := range options {
		opt.applyInsertOptions(&opts)
	}

	opts.params["query"] = fmt.Sprintf("INSERT INTO %s FORMAT %s", table, opts.format.String())

	req, err := c.newRequest(ctx, DiscoveryCtx{Kind: ClientKindInsert}, opts.params, opts.headers)
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
