package rowbinary

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKV_Get(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Test getting non-existent key
	val, ok := kv.Get("nonexistent")
	assert.False(ok)
	assert.Equal(0, val)

	// Set a value and get it
	kv.Set("key1", 42)
	val, ok = kv.Get("key1")
	assert.True(ok)
	assert.Equal(42, val)
}

func TestKV_Set(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Set new key
	result := kv.Set("key1", 42)
	assert.Equal(kv, result) // Should return the same KV for chaining

	val, ok := kv.Get("key1")
	assert.True(ok)
	assert.Equal(42, val)

	// Update existing key
	kv.Set("key1", 100)
	val, ok = kv.Get("key1")
	assert.True(ok)
	assert.Equal(100, val)
}

func TestKV_Delete(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Set some values
	kv.Set("key1", 42)
	kv.Set("key2", 84)

	// Delete existing key
	result := kv.Delete("key1")
	assert.Equal(kv, result)

	val, ok := kv.Get("key1")
	assert.False(ok)
	assert.Equal(0, val)

	// key2 should still exist
	val, ok = kv.Get("key2")
	assert.True(ok)
	assert.Equal(84, val)

	// Delete non-existent key (should not panic)
	result = kv.Delete("nonexistent")
	assert.Equal(kv, result)
}

func TestKV_Reset(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Set some values
	kv.Set("key1", 42)
	kv.Set("key2", 84)

	// Reset
	result := kv.Reset()
	assert.Equal(kv, result)

	// Should be empty
	val, ok := kv.Get("key1")
	assert.False(ok)
	assert.Equal(0, val)

	val, ok = kv.Get("key2")
	assert.False(ok)
	assert.Equal(0, val)
}

func TestKV_Each(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Set some values
	kv.Set("key1", 42)
	kv.Set("key2", 84)
	kv.Set("key3", 126)

	// Collect values using Each
	var keys []string
	var values []int
	err := kv.Each(func(key string, value int) error {
		keys = append(keys, key)
		values = append(values, value)
		return nil
	})
	assert.NoError(err)

	// Since order is not guaranteed, check lengths and contents
	assert.Len(keys, 3)
	assert.Len(values, 3)
	assert.Contains(keys, "key1")
	assert.Contains(keys, "key2")
	assert.Contains(keys, "key3")
	assert.Contains(values, 42)
	assert.Contains(values, 84)
	assert.Contains(values, 126)
}

func TestKV_MethodChaining(t *testing.T) {
	assert := assert.New(t)
	kv := &KV[string, int]{}

	// Test chaining
	kv.Set("a", 1).Set("b", 2).Delete("a").Reset()

	_, ok := kv.Get("a")
	assert.False(ok)

	_, ok = kv.Get("b")
	assert.False(ok)
}
