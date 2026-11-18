package rowbinary

import (
	"io"
	"net/http"
	"testing"
)

func TestStructureQuotesOnlyUnusualColumnNames(t *testing.T) {
	plain := NewFormatWriter(io.Discard, C("value", UInt64), C("_s2", String))
	if got := plain.Structure(); got != "value UInt64, _s2 String" {
		t.Errorf("plain names: %q", got)
	}
	unusual := NewFormatWriter(io.Discard, C("my col", UInt64), C("a, b", String), C("back`tick", UInt8), C("2x", UInt8))
	if got, want := unusual.Structure(), "`my col` UInt64, `a, b` String, `back\\`tick` UInt8, `2x` UInt8"; got != want {
		t.Errorf("unusual names:\n got %s\nwant %s", got, want)
	}
}

func TestExternalDataTableNamesValidatedBeforeSending(t *testing.T) {
	s := drainServer(t) // from callback_context_test.go
	requests := 0
	c := NewClient(WithDSN(s.URL), WithHTTPClient(&http.Client{Transport: countingTransport{&requests}}))
	writer := WithFormatWriter(func(w *FormatWriter) error { return Write(w, UInt8, 1) })
	for name, opts := range map[string][]SelectOption{
		"empty name": {WithExternalData("", C("x", UInt8), writer)},
		"duplicate":  {WithExternalData("t", C("x", UInt8), writer), WithExternalData("t", C("x", UInt8), writer)},
	} {
		if err := c.Select(t.Context(), "SELECT 1", opts...); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if requests != 0 {
		t.Errorf("%d requests sent for invalid external data", requests)
	}
}

type countingTransport struct{ n *int }

func (c countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	*c.n++
	return http.DefaultTransport.RoundTrip(r)
}
