package rowbinary

func putUvarint(buf []byte, x uint64) (int, error) {
	i := 0
	for x >= 0x80 {
		if i >= len(buf) {
			return 0, newOverflowError()
		}
		buf[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	if i >= len(buf) {
		return 0, newOverflowError()
	}
	buf[i] = byte(x)
	return i + 1, nil
}
