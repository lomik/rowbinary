// Package integration holds the tests that talk to a live ClickHouse server.
// It is a separate module so the root module does not depend on testcontainers.
package integration

import (
	"os"
	"testing"

	"github.com/lomik/rowbinary"
	"github.com/lomik/rowbinary/integration/chtest"
)

var testDSN string

func TestMain(m *testing.M) {
	os.Exit(chtest.Main(m, func(s chtest.Server) { testDSN = s.DSN }))
}

func integrationClient(t testing.TB) *rowbinary.TestClient {
	t.Helper()
	if testDSN == "" {
		t.Skip("offline mode: set ROWBINARY_TEST_MODE=integration for live ClickHouse tests")
	}
	return rowbinary.NewTestClientTB(t, testDSN)
}
