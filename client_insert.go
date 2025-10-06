package rowbinary

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
)

type insertOptions struct {
	dsn           string
	formatOptions []FormatOption
	format        Format
	params        map[string]string
	headers       map[string]string
	formatWriter  func(w *FormatWriter) error
	bodyWriter    func(w io.Writer) error
}

type InsertOption interface {
	applyInsertOptions(*insertOptions)
}

var _ InsertOption = C("", nil)
var _ InsertOption = WithUseBinaryHeader(false)
var _ InsertOption = RowBinary
var _ InsertOption = WithParam("key", "value")
var _ InsertOption = WithHeader("key", "value")
var _ InsertOption = WithDSN("http://localhost:8123")

func (c *client) Insert(ctx context.Context, table string, options ...InsertOption) error {
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

	req, err := c.newRequest(ctx, opts.dsn, DiscoveryCtx{Method: ClientMethodInsert}, opts.params, opts.headers)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	req.Body = r

	go func() {
		defer w.Close()
		bw := bufio.NewWriterSize(w, 1024*1024)
		defer bw.Flush()

		if opts.formatWriter != nil {
			writer := NewFormatWriter(bw, opts.formatOptions...)
			if err := writer.WriteHeader(); err != nil {
				w.CloseWithError(err)
				return
			}

			if err := opts.formatWriter(writer); err != nil {
				_ = w.CloseWithError(err)
				return
			}

			return
		}

		if opts.bodyWriter != nil {
			if err := opts.bodyWriter(bw); err != nil {
				_ = w.CloseWithError(err)
				return
			}
			return
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

type formatWriterOption struct {
	formatWriter func(w *FormatWriter) error
}

func WithFormatWriter(fw func(w *FormatWriter) error) InsertOption {
	return formatWriterOption{formatWriter: fw}
}

func (o formatWriterOption) applyInsertOptions(opts *insertOptions) {
	opts.formatWriter = o.formatWriter
}

type bodyWriterOption struct {
	bodyWriter func(w io.Writer) error
}

func WithBodyWriter(bw func(w io.Writer) error) InsertOption {
	return bodyWriterOption{bodyWriter: bw}
}

func (o bodyWriterOption) applyInsertOptions(opts *insertOptions) {
	opts.bodyWriter = o.bodyWriter
}
