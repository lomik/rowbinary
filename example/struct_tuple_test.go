package example

import (
	"testing"

	"github.com/lomik/rowbinary/rowbinarytest"
)

func TestStructTuple(t *testing.T) {
	rowbinarytest.TestType(t, StructTupleType, StructTuple{ID: 42, Name: []byte("test")}, "CREATE TEMPORARY TABLE tmp (value Tuple(id UInt32, name String)) ENGINE=Memory; INSERT INTO tmp VALUES ((42, 'test')); SELECT value FROM tmp")
}

func BenchmarkStructTuple(b *testing.B) {
	rowbinarytest.BenchmarkType(b, StructTupleType, StructTuple{ID: 42, Name: []byte("test")})
}
