package rowbinary

import (
	"fmt"
	"io"
	"net/http"
)

// maxErrorBody caps how much of an error response is read into memory.
// ClickHouse exception messages are far smaller.
const maxErrorBody = 64 << 10

// statusError returns the error for a non-200 response, quoting at most
// maxErrorBody bytes of its body.
func statusError(resp *http.Response) error {
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody+1))
	if err != nil {
		return fmt.Errorf("unexpected status code: %d (reading body: %w)", resp.StatusCode, err)
	}
	suffix := ""
	if len(body) > maxErrorBody {
		body, suffix = body[:maxErrorBody], "... (truncated)"
	}
	if loc, err := resp.Location(); resp.StatusCode/100 == 3 && err == nil {
		// Not followed: a streamed body (Insert, external data) cannot be
		// re-sent, or the client does not follow redirects.
		return fmt.Errorf("unexpected status code: %d (redirect to %s was not followed; put the final URL in the DSN)", resp.StatusCode, redirectTarget(loc))
	}
	return fmt.Errorf("unexpected status code: %d (%s%s)", resp.StatusCode, body, suffix)
}

// drainBody consumes a successful response without buffering it, so large
// results of Exec or Insert do not accumulate in memory, and read errors (a
// cut-off response) are still reported.
func drainBody(resp *http.Response) error {
	_, err := io.Copy(io.Discard, resp.Body)
	return err
}
