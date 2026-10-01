// hashkit is a built-in extension providing hashing and encoding
// utilities.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"

	"extsys/internal/extsdk"
)

func main() {
	version := os.Getenv("HASHKIT_VERSION")
	if version == "" {
		version = "0.9.1"
	}
	ext := extsdk.New("hashkit", version)

	ext.Handle("sha256", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		sum := sha256.Sum256([]byte(in.Text))
		return map[string]string{"hex": hex.EncodeToString(sum[:])}, nil
	})

	ext.Handle("reverse", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		r := []rune(in.Text)
		for i, j := 0, len(r)-1; i < j; i, j = i+1, j-1 {
			r[i], r[j] = r[j], r[i]
		}
		return map[string]string{"text": string(r)}, nil
	})

	if err := ext.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "hashkit:", err)
		os.Exit(1)
	}
}
