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

	// State keys this build can interpret; the upgrade preview checks
	// the old session's saved state against exactly this set.
	stateKeys := os.Getenv("TEXTKIT_STATE_KEYS")
	if stateKeys == "" {
		stateKeys = "docs_processed,upper_calls"
	}
	ext.DeclareStateKeys(strings.Split(stateKeys, ",")...)

	// ticks is a persistent subscription: a continuous stream of
	// identified events used to demonstrate subscription handoff
	// during upgrades and rollbacks.
	ext.DeclareSubscriptions("ticks")
	ext.Handle("emit", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Sub         string `json:"sub"`
			Count       int    `json:"count"`
			DelayMillis int    `json:"delay_millis"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		if in.Sub == "" {
			in.Sub = "ticks"
		}
		if in.Count <= 0 {
			in.Count = 1
		}
		go func() {
			for i := 0; i < in.Count; i++ {
				if in.DelayMillis > 0 {
					time.Sleep(time.Duration(in.DelayMillis) * time.Millisecond)
				}
				if _, err := ext.Emit(in.Sub, map[string]string{"source": "textkit"}); err != nil {
					return
				}
			}
		}()
		return map[string]string{"ok": "emitting"}, nil
	})

	ext.Handle("uppercase", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		n := 0
		if raw, ok := ext.State("upper_calls"); ok {
			_ = json.Unmarshal(raw, &n)
		}
		_ = ext.SetState("upper_calls", n+1)
		return map[string]string{"text": strings.ToUpper(in.Text)}, nil
	})

	ext.Handle("wordcount", 1, func(p json.RawMessage) (any, error) {
		var in struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(p, &in); err != nil {
			return nil, err
		}
		n := 0
		if raw, ok := ext.State("docs_processed"); ok {
			_ = json.Unmarshal(raw, &n)
		}
		_ = ext.SetState("docs_processed", n+1)
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
