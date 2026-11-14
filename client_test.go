package rowbinary

import (
	"os"
	"testing"

	"github.com/lomik/rowbinary/internal/chtest"
)

var testClickHouseDSN string

func TestMain(m *testing.M) {
	os.Exit(chtest.Main(m, func(s chtest.Server) { testClickHouseDSN = s.DSN }))
}

func integrationClient(t testing.TB) *TestClient {
	t.Helper()
	if testClickHouseDSN == "" {
		t.Skip("offline mode: set ROWBINARY_TEST_MODE=integration for live ClickHouse tests")
	}
	return NewTestClientTB(t, testClickHouseDSN)
}
