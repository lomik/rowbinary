package rowbinary

import (
	"bytes"
	"testing"
)

func TestVariantNull(t *testing.T) {
	tp := Variant(String, UInt32)
	t.Run("scan", func(t *testing.T) {
		got := Value{String, "previous"}
		if err := tp.Scan(NewReader(bytes.NewReader([]byte{255})), &got); err != nil {
			t.Fatal(err)
		}
		if !Eq(got.Type, Nothing) || got.Value != nil {
			t.Fatalf("got %#v, want Nothing(nil)", got)
		}
	})
	t.Run("write", func(t *testing.T) {
		var buf bytes.Buffer
		if err := tp.Write(NewWriter(&buf), Value{Nothing, nil}); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(buf.Bytes(), []byte{255}) {
			t.Fatalf("wire = %x, want ff", buf.Bytes())
		}
	})
}

func TestVariantCanonicalOrder(t *testing.T) {
	members := []Any{UInt32, String}
	tp := Variant(members...)
	for _, tc := range []struct {
		name  string
		value Value
		wire  []byte
	}{
		{"string", Value{String, "x"}, []byte{0, 1, 'x'}},
		{"uint32", Value{UInt32, uint32(42)}, []byte{1, 42, 0, 0, 0}},
	} {
		t.Run(tc.name+"/write", func(t *testing.T) {
			var buf bytes.Buffer
			if err := tp.Write(NewWriter(&buf), tc.value); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf.Bytes(), tc.wire) {
				t.Fatalf("wire = %x, want %x", buf.Bytes(), tc.wire)
			}
		})
		t.Run(tc.name+"/scan", func(t *testing.T) {
			var got Value
			if err := tp.Scan(NewReader(bytes.NewReader(tc.wire)), &got); err != nil {
				t.Fatal(err)
			}
			if !Eq(got.Type, tc.value.Type) || got.Value != tc.value.Value {
				t.Fatalf("got %#v, want %#v", got, tc.value)
			}
		})
	}
	if tp.String() != "Variant(String, UInt32)" {
		t.Errorf("noncanonical type: %s", tp.String())
	}
	if !bytes.Equal(tp.Binary(), Variant(String, UInt32).Binary()) {
		t.Error("binary type depends on constructor order")
	}
	if !Eq(members[0], UInt32) || !Eq(members[1], String) {
		t.Error("constructor mutated caller's slice")
	}
}
