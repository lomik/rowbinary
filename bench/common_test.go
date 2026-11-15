package bench

import (
	"os"
	"testing"

	"github.com/lomik/rowbinary"
	"github.com/lomik/rowbinary/integration/chtest"
)

var testClickHouseDSN, testClickHouseNativeAddr, testUsername, testPassword string

func TestMain(m *testing.M) {
	os.Exit(chtest.Main(m, func(s chtest.Server) {
		testClickHouseDSN, testClickHouseNativeAddr = s.DSN, s.NativeAddr
		testUsername, testPassword = s.Username, s.Password
	}))
}

func requireIntegration(b *testing.B) {
	b.Helper()
	if testClickHouseDSN == "" {
		b.Fatal("live benchmarks require ROWBINARY_TEST_MODE=integration")
	}
}

func nativeAddr(b *testing.B) string {
	b.Helper()
	requireIntegration(b)
	if testClickHouseNativeAddr == "" {
		b.Fatal("native benchmarks require ROWBINARY_TEST_NATIVE_ADDR with an external DSN")
	}
	return testClickHouseNativeAddr
}

func newTestClient(b *testing.B) *rowbinary.TestClient {
	b.Helper()
	requireIntegration(b)
	return rowbinary.NewTestClientTB(b, testClickHouseDSN)
}
