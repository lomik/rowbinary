package rowbinary

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
)

type typeEnum16 struct {
	mp1  map[int16]string
	mp2  map[string]int16
	keys []int16
}

func Enum16(v map[string]int16) Type[string] {
	t := typeEnum16{
		mp1:  make(map[int16]string),
		mp2:  make(map[string]int16),
		keys: make([]int16, 0, len(v)),
	}
	for k, v := range v {
		t.mp1[v] = k
		t.mp2[k] = v
		t.keys = append(t.keys, v)
	}
	slices.Sort(t.keys)
	return MakeTypeWrapAny[string](t)
}

func (t typeEnum16) String() string {
	var b strings.Builder
	b.WriteString("Enum16(")
	// TODO: Escape value
	for i, k := range t.keys {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(fmt.Sprintf("%s = %d", quote(t.mp1[k]), k))
	}
	b.WriteString(")")
	return b.String()
}

func (t typeEnum16) Binary() []byte {
	var b bytes.Buffer
	w := NewWriter(&b)
	w.Write(BinaryTypeEnum16[:])
	VarintWrite(w, uint64(len(t.keys)))
	for _, k := range t.keys {
		String.Write(w, t.mp1[k])
		Int16.Write(w, k)
	}
	return b.Bytes()
}

func (t typeEnum16) Write(w Writer, value string) error {
	v, ok := t.mp2[value]
	if !ok {
		return fmt.Errorf("invalid enum value %q", value)
	}

	return Int16.Write(w, v)
}

func (t typeEnum16) Scan(r Reader, ret *string) error {
	var v int16
	err := Int16.Scan(r, &v)
	if err != nil {
		return err
	}

	var ok bool
	*ret, ok = t.mp1[v]
	if !ok {
		return fmt.Errorf("invalid enum value %d", v)
	}
	return nil
}
