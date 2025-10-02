package example

import (
	"testing"

	"github.com/lomik/rowbinary"
)

func TestCustomTuple(t *testing.T) {
	rowbinary.TestType(t, CustomTupleType, CustomTuple{ID: 42, Name: "test"}, "CREATE TEMPORARY TABLE tmp (value Tuple(id UInt32, name String)) ENGINE=Memory; INSERT INTO tmp VALUES ((42, 'test')); SELECT value FROM tmp")
}

func BenchmarkCustomTuple(b *testing.B) {
	rowbinary.BenchmarkType(b, CustomTupleType, CustomTuple{ID: 42, Name: "test"})
}
