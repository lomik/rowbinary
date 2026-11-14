package example

import (
	"os"
	"testing"

	"github.com/lomik/rowbinary"
	"github.com/lomik/rowbinary/internal/chtest"
)

var testDSN string

func TestMain(m *testing.M) {
	os.Exit(chtest.Main(m, func(s chtest.Server) { testDSN = s.DSN }))
}

func TestStructTuple(t *testing.T) {
	rowbinary.TestTypeWithDSN(t, StructTupleType, StructTuple{ID: 42, Name: []byte("test")}, "CREATE TEMPORARY TABLE tmp (value Tuple(id UInt32, name String)) ENGINE=Memory; INSERT INTO tmp VALUES ((42, 'test')); SELECT value FROM tmp", testDSN)
}

func BenchmarkStructTuple(b *testing.B) {
	rowbinary.BenchmarkType(b, StructTupleType, StructTuple{ID: 42, Name: []byte("test")})
}
