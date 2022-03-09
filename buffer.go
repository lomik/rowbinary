package rowbinary

import (
	"encoding/binary"
	"math"
)

// Buffer is fixed size buffer for RowBinary data. It is not goroutine-safe
type Buffer struct {
	used int
	body []byte
	pool *Pool
}

// NewBuffer ...
func NewBuffer(size int) *Buffer {
	return &Buffer{
		body: make([]byte, size),
	}
}

// Reset ...
func (wb *Buffer) Reset() *Buffer {
	wb.used = 0
	return wb
}

// Len returns used size
func (wb *Buffer) Len() int {
	return wb.used
}

// Cap returns full size of buffer (used+unused)
func (wb *Buffer) Cap() int {
	return len(wb.body)
}

// Free returns unused size of buffer
func (wb *Buffer) Free() int {
	return len(wb.body) - wb.used
}

// IsEmpty ...
func (wb *Buffer) IsEmpty() bool {
	return wb.used == 0
}

// Bytes returns buffer body. It's valid until next write to buffer or reset
func (wb *Buffer) Bytes() []byte {
	return wb.body[:wb.used]
}

// Releases puts buffer back to pool
func (wb *Buffer) Release() {
	if wb.pool != nil {
		wb.pool.Put(wb)
	}
}

// WriteBytes ...
func (wb *Buffer) WriteBytes(p []byte) error {
	varintSize, err := putUvarint(wb.body[wb.used:], uint64(len(p)))
	if err != nil {
		return err
	}
	if len(p) > wb.Free()-varintSize {
		return newOverflowError()
	}
	wb.used += varintSize
	wb.used += copy(wb.body[wb.used:], p)
	return nil
}

// WriteString ...
func (wb *Buffer) WriteString(s string) error {
	return wb.WriteBytes([]byte(s))
}

// WriteUVarint ...
func (wb *Buffer) WriteUVarint(v uint64) error {
	varintSize, err := putUvarint(wb.body[wb.used:], v)
	if err != nil {
		return err
	}
	wb.used += varintSize
	return nil
}

// WriteFloat64 ...
func (wb *Buffer) WriteFloat64(value float64) error {
	if wb.Free() < 8 {
		return newOverflowError()
	}
	binary.LittleEndian.PutUint64(wb.body[wb.used:], math.Float64bits(value))
	wb.used += 8
	return nil
}

// WriteUint16 ...
func (wb *Buffer) WriteUint16(value uint16) error {
	if wb.Free() < 2 {
		return newOverflowError()
	}
	binary.LittleEndian.PutUint16(wb.body[wb.used:], value)
	wb.used += 2
	return nil
}

// WriteUint32 ...
func (wb *Buffer) WriteUint32(value uint32) error {
	if wb.Free() < 4 {
		return newOverflowError()
	}
	binary.LittleEndian.PutUint32(wb.body[wb.used:], value)
	wb.used += 4
	return nil
}

// WriteUint64 ...
func (wb *Buffer) WriteUint64(value uint64) error {
	if wb.Free() < 8 {
		return newOverflowError()
	}
	binary.LittleEndian.PutUint64(wb.body[wb.used:], value)
	wb.used += 8
	return nil
}

// Write raw RowBinary into buffer
func (wb *Buffer) Write(p []byte) error {
	if wb.Free() < len(p) {
		return newOverflowError()
	}
	wb.used += copy(wb.body[wb.used:], p)
	return nil
}

// Transaction ...
func (wb *Buffer) Transaction(cb func(*Buffer) error) error {
	currentUsed := wb.used
	err := cb(wb)
	if err != nil {
		// rollback
		wb.used = currentUsed
		return err
	}
	return nil
}
