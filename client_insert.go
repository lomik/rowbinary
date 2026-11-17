package rowbinary

import (
	"bufio"
	"context"
	"errors"
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
		if opt == nil {
			continue
		}
		opt.applyInsertOptions(&opts)
	}

	if opts.formatWriter == nil && opts.bodyWriter == nil {
		return errors.New("insert requires WithFormatWriter or WithBodyWriter")
	}

	opts.params["query"] = fmt.Sprintf("INSERT INTO %s FORMAT %s", table, opts.format.String())

	req, err := c.newRequest(ctx, opts.dsn, DiscoveryCtx{Method: ClientMethodInsert}, opts.params, opts.headers)
	if err != nil {
		return err
	}

	r, w := io.Pipe()
	defer r.Close()
	req.Body = r

	producerDone := make(chan error, 1)
	go func() {
		bw := bufio.NewWriterSize(w, 1024*1024)
		var producerErr error
		defer func() {
			if producerErr == nil {
				producerErr = bw.Flush()
			}
			// Publish the result before closing the pipe: the transport sees EOF
			// only after the close, so once the upload is complete the result is
			// already available and a late context cancellation cannot mask it.
			producerDone <- producerErr
			_ = w.CloseWithError(producerErr)
		}()

		if opts.formatWriter != nil {
			writer := NewFormatWriter(bw, opts.formatOptions...)
			if producerErr = writer.WriteHeader(); producerErr != nil {
				return
			}

			producerErr = opts.formatWriter(writer)

			return
		}

		if opts.bodyWriter != nil {
			producerErr = opts.bodyWriter(bw)
			return
		}
	}()

	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	if err := drainBody(resp); err != nil {
		return err
	}

	// An early response may stop the transport from consuming the request body.
	// Unblock any pending pipe write before waiting for its final result.
	_ = r.Close()
	select {
	case err := <-producerDone:
		return err
	case <-ctx.Done():
		// Both cases may be ready; a finished producer means the upload succeeded.
		select {
		case err := <-producerDone:
			return err
		default:
			return ctx.Err()
		}
	}
}

type formatWriterOption struct {
	formatWriter func(w *FormatWriter) error
}

func WithFormatWriter(fw func(w *FormatWriter) error) formatWriterOption {
	return formatWriterOption{formatWriter: fw}
}

func (o formatWriterOption) applyInsertOptions(opts *insertOptions) {
	opts.formatWriter = o.formatWriter
}

func (o formatWriterOption) applyExternalDataOption(opts *externalData) {
	opts.cb = o.formatWriter
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
