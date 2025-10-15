package rowbinary

import "sort"

type kvPair[K any, V any] struct {
	key   K
	value V
}

type KV[K any, V any] struct {
	pairs []kvPair[K, V]
}

func NewKV[K any, V any]() *KV[K, V] {
	return &KV[K, V]{
		pairs: make([]kvPair[K, V], 0),
	}
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
