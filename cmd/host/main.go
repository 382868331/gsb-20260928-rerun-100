// host is the main application. It loads the two built-in extensions as
// supervised subprocesses, dispatches tasks to them, demonstrates crash
// isolation, timeout supervision and session rotation on restart, then
// shuts everything down gracefully. Only local, pre-built extension
// binaries are executed; nothing is downloaded or fetched at runtime.
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"extsys/internal/host"
)

func mustJSON(raw json.RawMessage, err error) string {
	if err != nil {
		return "ERROR: " + err.Error()
	}
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return string(raw)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func main() {
	binDir := "bin"
	if len(os.Args) > 1 {
		binDir = os.Args[1]
	}
	if err := os.MkdirAll("state", 0o755); err != nil {
		log.Fatal(err)
	}
	journal, err := host.OpenJournal(filepath.Join("state", "tasks.jsonl"))
	if err != nil {
		log.Fatal(err)
	}
	defer journal.Close()

	manifests := []host.Manifest{
		{
			Name:     "hashkit",
			Path:     filepath.Join(binDir, "hashkit"),
			Provides: []string{"sha256", "reverse"},
		},
		{
			Name:     "textkit",
			Path:     filepath.Join(binDir, "textkit"),
			Provides: []string{"uppercase", "wordcount", "sleep", "panic"},
			// textkit's hashupper pipeline depends on hashkit, so it
			// must start after hashkit and only if sha256 is live.
			Requires: []string{"sha256"},
		},
	}

	mgr, err := host.New(manifests, host.Config{
		HandshakeTimeout: 3 * time.Second,
		TaskTimeout:      1500 * time.Millisecond,
		ShutdownGrace:    2 * time.Second,
		Journal:          journal,
	})
	if err != nil {
		log.Fatalf("manifest check failed before anything started: %v", err)
	}
	if err := mgr.Start(); err != nil {
		log.Fatalf("startup failed, rolled back: %v", err)
	}
	fmt.Println("[host] extensions started in dependency order: hashkit, textkit")

	// --- Normal task dispatch -------------------------------------
	fmt.Println("[task] uppercase ->", mustJSON(mgr.Call("uppercase", map[string]string{"text": "hello extensions"})))
	fmt.Println("[task] wordcount ->", mustJSON(mgr.Call("wordcount", map[string]string{"text": "one two three four"})))
	fmt.Println("[task] sha256    ->", mustJSON(mgr.Call("sha256", map[string]string{"text": "hello"})))
	fmt.Println("[task] reverse   ->", mustJSON(mgr.Call("reverse", map[string]string{"text": "abcdef"})))

	// --- Crash isolation ------------------------------------------
	fmt.Println("[task] panic     ->", mustJSON(mgr.Call("panic", map[string]string{})))
	fmt.Println("[host] textkit crashed; hashkit still serving:",
		mustJSON(mgr.Call("sha256", map[string]string{"text": "after-crash"})))

	// --- Restart with a fresh session -----------------------------
	oldSession := mgr.Session("textkit")
	if err := mgr.Restart("textkit"); err != nil {
		log.Fatalf("restart textkit: %v", err)
	}
	newSession := mgr.Session("textkit")
	fmt.Printf("[host] textkit restarted: old session %s... retired, new session %s...\n",
		oldSession[:8], newSession[:8])
	fmt.Println("[task] uppercase after restart ->",
		mustJSON(mgr.Call("uppercase", map[string]string{"text": "back online"})))

	// --- Timeout supervision --------------------------------------
	fmt.Println("[task] sleep(5s, timeout 1.5s) ->", mustJSON(mgr.Call("sleep", map[string]int{"millis": 5000})))
	fmt.Println("[host] still alive after slow task; other extension unaffected:",
		mustJSON(mgr.Call("sha256", map[string]string{"text": "after-timeout"})))

	// --- Graceful shutdown ----------------------------------------
	fmt.Println("[host] shutting down: stop accepting, drain, exit")
	mgr.Shutdown()
	fmt.Println("[host] all extensions exited cleanly; task journal at state/tasks.jsonl")
}
