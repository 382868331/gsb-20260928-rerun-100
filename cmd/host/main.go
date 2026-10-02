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

	// --- Dependency-ordered upgrade -------------------------------
	// Both extensions move to a new build (same trusted local binary,
	// new version stamp). Old processes keep serving while candidates
	// are spawned and verified; only after every candidate passes the
	// handshake does traffic cut over.
	oldTextkit, oldHashkit := mgr.Session("textkit"), mgr.Session("hashkit")
	fmt.Println("[task] uppercase before upgrade ->",
		mustJSON(mgr.Call("uppercase", map[string]string{"text": "served by old build"})))
	upgrade := []host.Manifest{
		{
			Name:     "hashkit",
			Path:     filepath.Join(binDir, "hashkit"),
			Env:      []string{"HASHKIT_VERSION=0.9.2"},
			Provides: []string{"sha256", "reverse"},
		},
		{
			Name: "textkit",
			Path: filepath.Join(binDir, "textkit"),
			// 1.3.0 no longer understands the legacy upper_calls key,
			// so the preview reports it as incompatible.
			Env:      []string{"TEXTKIT_VERSION=1.3.0", "TEXTKIT_STATE_KEYS=docs_processed"},
			Provides: []string{"uppercase", "wordcount", "sleep", "panic"},
			Requires: []string{"sha256"},
		},
	}
	// Prepare the upgrade and preview the session-state migration:
	// what the old sessions saved, what the candidates can accept, and
	// what they cannot interpret.
	plan, err := mgr.PrepareUpgrade(upgrade)
	if err != nil {
		log.Fatalf("upgrade preparation failed, old combination must still serve: %v", err)
	}
	decisions := map[string]host.Disposition{}
	for _, s := range plan.Sessions {
		fmt.Printf("[state] %s %s -> %s: old session %s... holds %d items, candidate accepts %v, rejects %v\n",
			s.Extension, s.OldVersion, s.NewVersion, s.OldSession[:8], len(s.Items), s.Accepted, s.Rejected)
		for _, it := range s.Items {
			fmt.Printf("[state]   item %-16s v%d %s\n", it.Key, it.Version, string(it.Value))
		}
		if len(s.Rejected) > 0 {
			// Explicit user decision: migrate the accepted items and
			// drop what the new build cannot interpret.
			fmt.Printf("[state] %s: %d incompatible items, deciding migrate (accepted kept, rejected dropped)\n",
				s.Extension, len(s.Rejected))
			decisions[s.Extension] = host.DecideMigrate
		}
	}
	if err := mgr.ConfirmUpgrade(plan.ID, decisions); err != nil {
		log.Fatalf("upgrade confirmation failed, old combination must still serve: %v", err)
	}
	fmt.Printf("[host] upgrade cut over: textkit %s... -> %s..., hashkit %s... -> %s...\n",
		oldTextkit[:8], mgr.Session("textkit")[:8], oldHashkit[:8], mgr.Session("hashkit")[:8])
	for _, session := range []string{oldTextkit, oldHashkit} {
		if owner, ok := mgr.SessionOwner(session); ok {
			fmt.Printf("[owner] session %s... -> %s %s active=%v (%s)\n",
				session[:8], owner.Extension, owner.Version, owner.Active, owner.Note)
		}
	}
	if owner, ok := mgr.SessionOwner(mgr.Session("textkit")); ok {
		fmt.Printf("[owner] session %s... -> %s %s active=%v (%s)\n",
			mgr.Session("textkit")[:8], owner.Extension, owner.Version, owner.Active, owner.Note)
	}
	if items, err := mgr.DumpState("textkit"); err == nil {
		for _, it := range items {
			fmt.Printf("[state] textkit now holds %-16s v%d %s\n", it.Key, it.Version, string(it.Value))
		}
	}
	fmt.Println("[task] uppercase after upgrade ->",
		mustJSON(mgr.Call("uppercase", map[string]string{"text": "served by new build"})))

	// --- Failed upgrade keeps the old combination -------------------
	// This candidate declares a capability the binary does not
	// implement, so it is rejected during preparation. The running
	// versions are never touched.
	broken := []host.Manifest{{
		Name:     "textkit",
		Path:     filepath.Join(binDir, "textkit"),
		Env:      []string{"TEXTKIT_VERSION=9.9.9"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic", "fly"},
		Requires: []string{"sha256"},
	}}
	before := mgr.Session("textkit")
	if err := mgr.Upgrade(broken); err != nil {
		fmt.Println("[host] bad candidate rejected, old combination retained:", err)
	}
	if mgr.Session("textkit") != before {
		log.Fatal("failed upgrade must not replace the running session")
	}
	fmt.Println("[task] uppercase after failed upgrade ->",
		mustJSON(mgr.Call("uppercase", map[string]string{"text": "still the retained build"})))

	// --- Rollback with a state-compatibility report -----------------
	// Rolling back is not just "start the old process again": the old
	// build must be able to receive the state the new one accumulated.
	report, err := mgr.Rollback("textkit", host.Manifest{
		Name:     "textkit",
		Path:     filepath.Join(binDir, "textkit"),
		Env:      []string{"TEXTKIT_VERSION=1.2.0"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic"},
		Requires: []string{"sha256"},
	})
	if err != nil {
		log.Fatalf("rollback failed: %v", err)
	}
	fmt.Printf("[rollback] textkit %s -> %s: state fully restored=%v accepted=%v rejected=%v\n",
		report.FromVersion, report.ToVersion, report.StateFullyAccepted, report.Accepted, report.Rejected)
	if !report.StateFullyAccepted {
		fmt.Println("[rollback] WARNING: old build could not receive all state; session state only partially restored")
	}

	// --- Handoff audit trail ----------------------------------------
	fmt.Println("[host] upgrade audit trail:")
	for _, rec := range journal.SwitchRecords() {
		fmt.Printf("  [%s] %-9s %-8s old=%s(%s) new=%s(%s) %s\n",
			rec.UpgradeID, rec.Event, rec.Extension,
			short(rec.OldVersion), short(rec.OldSession),
			short(rec.NewVersion), short(rec.NewSession), rec.Detail)
	}

	// --- Timeout supervision --------------------------------------
	fmt.Println("[task] sleep(5s, timeout 1.5s) ->", mustJSON(mgr.Call("sleep", map[string]int{"millis": 5000})))
	fmt.Println("[host] still alive after slow task; other extension unaffected:",
		mustJSON(mgr.Call("sha256", map[string]string{"text": "after-timeout"})))

	// --- Graceful shutdown ----------------------------------------
	fmt.Println("[host] shutting down: stop accepting, drain, exit")
	mgr.Shutdown()
	fmt.Println("[host] all extensions exited cleanly; task journal at state/tasks.jsonl")
}

func short(s string) string {
	if s == "" {
		return "-"
	}
	if len(s) > 8 {
		return s[:8]
	}
	return s
}
