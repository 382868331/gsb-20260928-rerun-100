package host

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func startPersistentManager(t *testing.T, stateDir string) *Manager {
	t.Helper()
	journal, err := OpenJournal(filepath.Join(stateDir, "tasks.jsonl"))
	if err != nil {
		t.Fatalf("OpenJournal: %v", err)
	}
	mgr, err := New([]Manifest{streamManifest("a", "s1", "s2")}, Config{
		HandshakeTimeout: 2 * time.Second,
		TaskTimeout:      2 * time.Second,
		ShutdownGrace:    time.Second,
		Journal:          journal,
		StateDir:         stateDir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		mgr.Shutdown()
		_ = journal.Close()
	})
	return mgr
}

func restartPersistentManager(t *testing.T, stateDir string) *Manager {
	t.Helper()
	mgr := startPersistentManager(t, stateDir)
	mgr.Shutdown()
	return startPersistentManager(t, stateDir)
}

func assertSubscriptionState(t *testing.T, mgr *Manager, subCount int, eventID string, status string) {
	t.Helper()
	subs, events, err := mgr.DumpSubscriptions("a")
	if err != nil {
		t.Fatalf("DumpSubscriptions: %v", err)
	}
	if len(subs) != subCount {
		t.Fatalf("restored subscriptions=%d want %d: %+v", len(subs), subCount, subs)
	}
	found := false
	for _, ev := range events {
		if ev.SubscriptionID == "s2" && ev.EventID == eventID {
			found = true
			if ev.Status != status {
				t.Fatalf("event %s status=%s want %s: %+v", eventID, ev.Status, status, ev)
			}
		}
	}
	if !found {
		t.Fatalf("event %s/%s was not restored: %+v", "s2", eventID, events)
	}
}

func TestRestartAfterNormalShutdownRestoresSubscriptionsAndProgress(t *testing.T) {
	stateDir := t.TempDir()
	mgr := startPersistentManager(t, stateDir)
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s2", "source": "virtual", "cursor": "10"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "report", "sub_id": "s2", "event": "pending", "pos": 3})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 4})
	mgr.Shutdown()

	mgr = startPersistentManager(t, stateDir)
	assertSubscriptionState(t, mgr, 1, "pending", "processing")
	assertSubscriptionState(t, mgr, 1, "done", "confirmed")
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 4})
	if out["ok"] != "duplicate" {
		t.Fatalf("confirmed event must not be processed again after restart: %+v", out)
	}
	if records := mgr.cfg.Journal.TaskRecords(); len(records) == 0 {
		t.Fatal("task journal history must reopen")
	}
}

func TestRestartIgnoresIncompleteFinalJournalAndStateLines(t *testing.T) {
	stateDir := t.TempDir()
	mgr := startPersistentManager(t, stateDir)
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s2", "source": "virtual", "cursor": "20"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 5})
	mgr.Shutdown()

	for _, name := range []string{"tasks.jsonl", "host-state.jsonl"} {
		path := filepath.Join(stateDir, name)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if err := os.WriteFile(path, append(b, []byte("{\"incomplete\":")...), 0o644); err != nil {
			t.Fatalf("truncate-append %s: %v", name, err)
		}
	}

	mgr = startPersistentManager(t, stateDir)
	assertSubscriptionState(t, mgr, 1, "done", "confirmed")
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 5})
	if out["ok"] != "duplicate" {
		t.Fatalf("confirmed event survives a torn log tail: %+v", out)
	}
}

func TestRestartAfterConsecutiveUpgradesUsesFinalVersionAndRestoresWatermark(t *testing.T) {
	stateDir := t.TempDir()
	mgr := startPersistentManager(t, stateDir)
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s2", "source": "virtual", "cursor": "30"})
	first := withEnv(streamManifest("a", "s1", "s2"), "EXTSYS_TEST_VER=2.0.0")
	plan, err := mgr.PrepareUpgrade([]Manifest{first})
	if err != nil {
		t.Fatalf("first PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err != nil {
		t.Fatalf("first ConfirmUpgrade: %v", err)
	}
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 6})
	second := withEnv(streamManifest("a", "s1", "s2"), "EXTSYS_TEST_VER=3.0.0")
	plan, err = mgr.PrepareUpgrade([]Manifest{second})
	if err != nil {
		t.Fatalf("second PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err != nil {
		t.Fatalf("second ConfirmUpgrade: %v", err)
	}
	mgr.Shutdown()

	mgr = startPersistentManager(t, stateDir)
	version, ok := mgr.SessionOwner(mgr.Session("a"))
	if !ok || !version.Active || version.Version != "3.0.0" {
		t.Fatalf("restart must restore final responsible version: ok=%v owner=%+v", ok, version)
	}
	assertSubscriptionState(t, mgr, 1, "done", "confirmed")
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "done", "pos": 6})
	if out["ok"] != "duplicate" {
		t.Fatalf("confirmation watermark must survive consecutive upgrades and restart: %+v", out)
	}
}
