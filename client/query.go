package client

import (
	"bufio"
	"context"
	"encoding/base64"
	"net/url"

	"github.com/valyala/fasthttp"
)

type query struct {
	bodyWriter func(w *bufio.Writer)
}

func (c *Client) do(req *fasthttp.Request, resp *fasthttp.Response) error {
	if c.opts.HTTPClient != nil {
		return c.opts.HTTPClient.Do(req, resp)
	}

	return fasthttp.Do(req, resp)
}

func basicAuth(username, password string) string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+password))
}

func (c *Client) prepare(ctx context.Context, req *fasthttp.Request) error {
	u, err := url.Parse(c.dsn)
	if err != nil {
		return err
	}

	uri := &url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
	}

	params := url.Values{}
	params.Set("output_format_binary_encode_types_in_binary_format", "1")
	params.Set("input_format_binary_decode_types_in_binary_format", "1")
	uri.RawQuery = params.Encode()

	req.SetRequestURI(uri.String())
	req.Header.SetProtocol("HTTP/1.1")
	req.Header.SetMethod("POST")
	req.Header.Set("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes")

	if u.User != nil {
		p, _ := u.User.Password()
		req.Header.Set("Authorization", basicAuth(u.User.Username(), p))
	}

	return nil
}
