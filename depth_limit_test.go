package rowbinary

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func nestedArrayBinary(levels int) []byte {
	return append(bytes.Repeat([]byte{0x1e}, levels), BinaryTypeUInt8[0]) // Array(...(UInt8))
}

func nestedArrayName(levels int) string {
	return strings.Repeat("Array(", levels) + "UInt8" + strings.Repeat(")", levels)
}

func TestTypeDepthLimit(t *testing.T) {
	if _, err := DecodeBinaryType(NewReader(bytes.NewReader(nestedArrayBinary(maxTypeDepth)))); err != nil {
		t.Errorf("binary, %d levels: %v", maxTypeDepth, err)
	}
	if _, err := DecodeBinaryType(NewReader(bytes.NewReader(nestedArrayBinary(maxTypeDepth + 1)))); err == nil {
		t.Errorf("binary, %d levels accepted", maxTypeDepth+1)
	}
	if _, err := DecodeStringType(nestedArrayName(maxTypeDepth)); err != nil {
		t.Errorf("text, %d levels: %v", maxTypeDepth, err)
	}
	if _, err := DecodeStringType(nestedArrayName(maxTypeDepth + 1)); err == nil {
		t.Errorf("text, %d levels accepted", maxTypeDepth+1)
	}
	// Brackets inside quoted enum labels do not count as nesting.
	if _, err := DecodeStringType("Enum8('" + strings.Repeat("(", 5000) + "' = 1)"); err != nil {
		t.Errorf("quoted brackets: %v", err)
	}
}

func TestVeryDeepTypesFailFast(t *testing.T) {
	const levels = 1_000_000
	start := time.Now()
	if _, err := DecodeBinaryType(NewReader(bytes.NewReader(nestedArrayBinary(levels)))); err == nil {
		t.Error("binary: a million levels accepted")
	}
	if _, err := DecodeStringType(nestedArrayName(levels)); err == nil {
		t.Error("text: a million levels accepted")
	}
	// The same through Dynamic, which decodes a type for every value.
	var v Value
	if err := DynamicDefault.Scan(NewReader(bytes.NewReader(nestedArrayBinary(levels))), &v); err == nil {
		t.Error("Dynamic: a million levels accepted")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Errorf("rejecting took %s", d)
	}
}
