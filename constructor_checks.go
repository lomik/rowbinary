package rowbinary

import (
	"cmp"
	"fmt"
	"slices"
)

// maxVariantTypes is the most member types ClickHouse allows in a Variant
// (checked against 25.9); discriminator 255 is reserved for NULL.
const maxVariantTypes = 255

// enumDuplicateValue returns a description of the first value (in label
// order) that two labels share, or "" if all values are distinct. ClickHouse
// rejects such enums ("Duplicate values in enum").
func enumDuplicateValue[V cmp.Ordered](name string, labels map[string]V) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	seen := make(map[V]string, len(labels))
	for _, k := range keys {
		v := labels[k]
		if prev, ok := seen[v]; ok {
			return fmt.Sprintf("%s value %v is used by both %s and %s", name, v, quote(prev), quote(k))
		}
		seen[v] = k
	}
	return ""
}

// dedupTypes drops repeated types (by ID), keeping the first occurrence, as
// ClickHouse does for Variant(UInt8, UInt8).
func dedupTypes(types []Any) []Any {
	seen := make(map[uint64]bool, len(types))
	out := types[:0]
	for _, tp := range types {
		if !seen[tp.ID()] {
			seen[tp.ID()] = true
			out = append(out, tp)
		}
	}
	return out
}

// enumBuilder collects decoded enum elements, rejecting duplicate names and
// values as ClickHouse does ("Duplicate names in enum", "Duplicate values in
// enum") in constant time per element.
type enumBuilder[V comparable] struct {
	kind   string
	labels map[string]V
	values map[V]string
}

func newEnumBuilder[V comparable](kind string) *enumBuilder[V] {
	return &enumBuilder[V]{kind: kind, labels: map[string]V{}, values: map[V]string{}}
}

func (b *enumBuilder[V]) add(name string, value V) error {
	if _, dup := b.labels[name]; dup {
		return fmt.Errorf("%s: duplicate name %s", b.kind, quote(name))
	}
	if other, dup := b.values[value]; dup {
		return fmt.Errorf("%s: value %v is used by both %s and %s", b.kind, value, quote(other), quote(name))
	}
	b.labels[name] = value
	b.values[value] = name
	return nil
}

func checkVariantCount(n uint64) error {
	if n > maxVariantTypes {
		return fmt.Errorf("Variant can have at most %d types, got %d", maxVariantTypes, n)
	}
	return nil
}
