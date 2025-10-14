package rowbinary

import "sort"

type kvPair[K comparable, V any] struct {
	key   K
	value V
}

type KV[K comparable, V any] struct {
	pairs []kvPair[K, V]
}

func NewKV[K comparable, V any]() *KV[K, V] {
	return &KV[K, V]{
		pairs: make([]kvPair[K, V], 0),
	}
}

func (kv *KV[K, V]) Get(key K) (V, bool) {
	for _, pair := range kv.pairs {
		if pair.key == key {
			return pair.value, true
		}
	}
	var zero V
	return zero, false
}

func (kv *KV[K, V]) Set(key K, value V) *KV[K, V] {
	for i, pair := range kv.pairs {
		if pair.key == key {
			kv.pairs[i].value = value
			return kv
		}
	}
	kv.pairs = append(kv.pairs, kvPair[K, V]{key: key, value: value})
	return kv
}

func (kv *KV[K, V]) Len() int {
	return len(kv.pairs)
}

func (kv *KV[K, V]) Append(key K, value V) *KV[K, V] {
	kv.pairs = append(kv.pairs, kvPair[K, V]{key: key, value: value})
	return kv
}

func (kv *KV[K, V]) Sort(less func(a, b K) bool) *KV[K, V] {
	sort.Slice(kv.pairs, func(i, j int) bool {
		return less(kv.pairs[i].key, kv.pairs[j].key)
	})
	return kv
}

func (kv *KV[K, V]) Delete(key K) *KV[K, V] {
	for i, pair := range kv.pairs {
		if pair.key == key {
			if i+1 == len(kv.pairs) {
				kv.pairs = kv.pairs[:i]
				return kv
			}

			kv.pairs = append(kv.pairs[:i], kv.pairs[i+1:]...)
			return kv
		}
	}
	return kv
}

func (kv *KV[K, V]) Reset() *KV[K, V] {
	kv.pairs = kv.pairs[:0]
	return kv
}

func (kv *KV[K, V]) Each(f func(key K, value V) error) error {
	for _, pair := range kv.pairs {
		err := f(pair.key, pair.value)
		if err != nil {
			return err
		}
	}
	return nil
}
