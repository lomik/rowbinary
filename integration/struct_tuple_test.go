package integration

import (
	"testing"

	"github.com/lomik/rowbinary/rowbinarytest"
	"github.com/lomik/rowbinary/example"
)

func TestStructTuple(t *testing.T) {
	rowbinarytest.TestTypeWithDSN(t, example.StructTupleType, example.StructTuple{ID: 42, Name: []byte("test")}, "CREATE TEMPORARY TABLE tmp (value Tuple(id UInt32, name String)) ENGINE=Memory; INSERT INTO tmp VALUES ((42, 'test')); SELECT value FROM tmp", testDSN)
}
