// textkit is a built-in extension providing text utilities. It also
// implements "sleep" and "panic" so the host can demonstrate timeout
// supervision and crash isolation without running any untrusted code.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"extsys/internal/extsdk"
)

func main() {
	version := os.Getenv("TEXTKIT_VERSION")
	if version == "" {
		version = "1.2.0"
	}
	ext := extsdk.New("textkit", version)

	// Stateful session: a counter plus a legacy note. Newer builds
	// (TEXTKIT_DROP_LEGACY=1) no longer understand the legacy note, so
	// upgrades must surface it as an incompatible state item.
	counter := 0
	ext.Handle("count", 1, func(p json.RawMessage) (any, error) {
		counter++
		return map[string]int{"count": counter}, nil
	})
	ext.StateHooks(
		func() map[string]json.RawMessage {
			n, _ := json.Marshal(counter)
			return map[string]json.RawMessage{
				"counter":     n,
				"legacy_note": json.RawMessage(`"draft-format-v1"`),
			}
		},
		func(items map[string]json.RawMessage) (map[string]json.RawMessage, map[string]string) {
			accepted := map[string]json.RawMessage{}
			rejected := map[string]string{}
			for k, v := range items {
				if k == "legacy_note" && os.Getenv("TEXTKIT_DROP_LEGACY") == "1" {
					rejected[k] = "draft-format-v1 notes are not supported by this build"
					continue
				}
				accepted[k] = v
			}
			return accepted, rejected
		},
		func(items map[string]json.RawMessage) error {
			if raw, ok := items["counter"]; ok {
				var n int
				if err := json.Unmarshal(raw, &n); err != nil {
					return err
				}
				counter = n
			}
			return nil
		},
	)

	ext.Handle("uppercase", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		return map[string]string{"text": strings.ToUpper(in.Text)}, nil
	})

	ext.Handle("wordcount", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		return map[string]int{"words": len(strings.Fields(in.Text))}, nil
	})

	// sleep simulates a slow task; the host must time it out and stay
	// responsive.
	ext.Handle("sleep", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Millis int `json:"millis"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		time.Sleep(time.Duration(in.Millis) * time.Millisecond)
		return map[string]string{"slept": fmt.Sprintf("%dms", in.Millis)}, nil
	})

	// panic kills the process abruptly; the host must isolate the
	// failure to this extension's pending requests.
	ext.Handle("panic", 1, func(p json.RawMessage) (any, error) {
		os.Exit(2)
		return nil, nil
	})

	if err := ext.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "textkit:", err)
		os.Exit(1)
	}
}
