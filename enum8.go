package rowbinary

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
)

type typeEnum8 struct {
	mp1  map[int8]string
	mp2  map[string]int8
	keys []int8
}

func Enum8(v map[string]int8) Type[string] {
	t := typeEnum8{
		mp1:  make(map[int8]string),
		mp2:  make(map[string]int8),
		keys: make([]int8, 0, len(v)),
	}
	for k, v := range v {
		t.mp1[v] = k
		t.mp2[k] = v
		t.keys = append(t.keys, v)
	}
	slices.Sort(t.keys)
	return MakeTypeWrapAny[string](t)
}

func (t typeEnum8) String() string {
	var b strings.Builder
	b.WriteString("Enum8(")
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

func (t typeEnum8) Binary() []byte {
	var b bytes.Buffer
	w := NewWriter(&b)
	w.Write(BinaryTypeEnum8[:])
	VarintWrite(w, uint64(len(t.keys)))
	for _, k := range t.keys {
		String.Write(w, t.mp1[k])
		Int8.Write(w, k)
	}
	return b.Bytes()
}

func (t typeEnum8) Write(w Writer, value string) error {
	v, ok := t.mp2[value]
	if !ok {
		return fmt.Errorf("invalid enum value %q", value)
	}

	return Int8.Write(w, v)
}

func (t typeEnum8) Scan(r Reader, v *string) error {
	var val int8
	err := Int8.Scan(r, &val)
	if err != nil {
		return err
	}

	var ok bool
	*v, ok = t.mp1[val]
	if !ok {
		return fmt.Errorf("invalid enum value %d", val)
	}
	return nil
}
