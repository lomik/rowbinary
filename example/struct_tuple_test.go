package example

import (
	"testing"

	"github.com/lomik/rowbinary"
)

func TestStructTuple(t *testing.T) {
	rowbinary.TestType(t, StructTupleType, StructTuple{ID: 42, Name: []byte("test")}, "CREATE TEMPORARY TABLE tmp (value Tuple(id UInt32, name String)) ENGINE=Memory; INSERT INTO tmp VALUES ((42, 'test')); SELECT value FROM tmp")
}

func BenchmarkStructTuple(b *testing.B) {
	rowbinary.BenchmarkType(b, StructTupleType, StructTuple{ID: 42, Name: []byte("test")})
}
