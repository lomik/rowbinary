package rowbinary

import "bytes"

func StringWrite(w Writer, value string) error {
	err := VarintWrite(w, uint64(len(value)))
	if err != nil {
		return err
	}
	_, err = w.Write(toBytes(value))
	return err
}

func StringEncode(s string) []byte {
	var b bytes.Buffer
	w := NewWriter(&b)
	StringWrite(w, s)
	return b.Bytes()
}
