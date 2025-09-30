package example

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
)

// requests clickhouse, caching locally to disk
// re-running the test can already work without CH. including in CI if you commit fixtures/*
func execLocal(query string) ([]byte, error) {
	h := sha256.New()
	h.Write([]byte(query))
	key := fmt.Sprintf("%x", h.Sum(nil))
	filename := fmt.Sprintf("fixtures/ch_%s.bin", key)

	// fmt.Println(filename, query)

	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		body, err := exec.Command("clickhouse", "local", "--query", query).Output()
		if err != nil {
			return nil, err
		}

		err = os.WriteFile(filename, body, 0600)
		return body, err
	}
	// #nosec G304
	return os.ReadFile(filename)
}

func pointer[V any](v V) *V {
	return &v
}

func null[V any](_ V) *V {
	return nil
}
