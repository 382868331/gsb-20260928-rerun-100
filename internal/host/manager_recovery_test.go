package host

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func openTestRecovery(t *testing.T, dir string) *RecoveryLog {
	t.Helper()
	rec, err := OpenRecoveryLog(filepath.Join(dir, "recovery.jsonl"))
	if err != nil {
		t.Fatalf("OpenRecoveryLog: %v", err)
	}
	return rec
}

func TestRecoveryAfterCleanShutdown(t *testing.T) {
	dir := t.TempDir()
	rec := openTestRecovery(t, dir)
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec}
	mgr := testManager(t, []Manifest{streamManifest("a", "s1")}, cfg)
	oldSession := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual", "cursor": "0"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "report", "sub_id": "s1", "event": "e2", "pos": 2})
	mgr.Shutdown()
	if err := rec.Close(); err != nil {
		t.Fatalf("close recovery log: %v", err)
	}

	// Restart the whole host against the same state directory.
	rec2 := openTestRecovery(t, dir)
	cfg2 := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec2}
	mgr2 := testManager(t, []Manifest{streamManifest("a", "s1")}, cfg2)
	if mgr2.Session("a") == oldSession {
		t.Fatal("restart must negotiate a fresh session token")
	}

	subs, events, err := mgr2.DumpSubscriptions("a")
	if err != nil {
		t.Fatalf("DumpSubscriptions after restart: %v", err)
	}
	if len(subs) != 1 || subs[0].ID != "s1" || subs[0].Source != "virtual" {
		t.Fatalf("subscription scope must survive host restart: %+v", subs)
	}
	status := map[string]string{}
	for _, ev := range events {
		status[ev.EventID] = ev.Status
	}
	if status["e1"] != "confirmed" || status["e2"] != "processing" {
		t.Fatalf("event ledger must survive host restart: %+v", events)
	}

	// A confirmed event must never be confirmed again after restart.
	out := streamCall(t, mgr2, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	if out["ok"] != "duplicate" {
		t.Fatalf("confirmed e1 must dedupe after restart: %+v", out)
	}
	// A pending event resumes as pending: it can still be confirmed.
	out = streamCall(t, mgr2, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e2", "pos": 2})
	if out["ok"] != "confirmed" {
		t.Fatalf("pending e2 must remain confirmable after restart: %+v", out)
	}

	// The last responsible version of the retired session is history,
	// not forgotten.
	owner, ok := mgr2.SessionOwner(oldSession)
	if !ok || owner.Active || owner.Extension != "a" || owner.Version != "3.1.4-test" {
		t.Fatalf("old session ownership must survive restart: %+v ok=%v", owner, ok)
	}
	if err := rec2.Close(); err != nil {
		t.Fatalf("close second recovery log: %v", err)
	}
}

func TestRecoveryToleratesTruncatedTail(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "recovery.jsonl")
	rec := openTestRecovery(t, dir)
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec}
	mgr := testManager(t, []Manifest{streamManifest("a", "s1")}, cfg)
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	mgr.Shutdown()
	if err := rec.Close(); err != nil {
		t.Fatalf("close recovery log: %v", err)
	}

	// Simulate a crash mid-write: a torn final line.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	if _, err := f.WriteString("{\"kind\":\"event\",\"extension\":\"a\",\"event\":{\"sub"); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close log: %v", err)
	}

	rec2, err := OpenRecoveryLog(path)
	if err != nil {
		t.Fatalf("reopen with torn tail must not fail: %v", err)
	}
	if subs := rec2.Subscriptions("a"); len(subs) != 1 || subs[0].ID != "s1" {
		t.Fatalf("complete records before the torn tail must survive: %+v", subs)
	}
	events := rec2.Events("a")
	if len(events) != 1 || events[0].EventID != "e1" || events[0].Status != "confirmed" {
		t.Fatalf("confirmed event before the torn tail must survive: %+v", events)
	}

	// The log keeps accepting records and replays cleanly afterwards.
	cfg2 := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec2}
	mgr2 := testManager(t, []Manifest{streamManifest("a", "s1")}, cfg2)
	out := streamCall(t, mgr2, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	if out["ok"] != "duplicate" {
		t.Fatalf("dedupe must work after tail repair: %+v", out)
	}
	streamCall(t, mgr2, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e2", "pos": 2})
	mgr2.Shutdown()
	if err := rec2.Close(); err != nil {
		t.Fatalf("close repaired log: %v", err)
	}

	rec3, err := OpenRecoveryLog(path)
	if err != nil {
		t.Fatalf("reopen repaired log: %v", err)
	}
	defer rec3.Close()
	got := map[string]string{}
	for _, ev := range rec3.Events("a") {
		got[ev.EventID] = ev.Status
	}
	if got["e1"] != "confirmed" || got["e2"] != "confirmed" {
		t.Fatalf("repaired log must replay every record: %+v", got)
	}
}

func TestRecoveryAfterConsecutiveUpgrades(t *testing.T) {
	dir := t.TempDir()
	rec := openTestRecovery(t, dir)
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec}
	mgr := testManager(t, []Manifest{streamManifest("a", "s1")}, cfg)
	first := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade v2: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err != nil {
		t.Fatalf("ConfirmUpgrade v2: %v", err)
	}
	second := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e2", "pos": 2})

	plan2, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1"), "EXTSYS_TEST_VER=3.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade v3: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan2.ID, nil); err != nil {
		t.Fatalf("ConfirmUpgrade v3: %v", err)
	}
	third := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e3", "pos": 3})
	mgr.Shutdown()
	if err := rec.Close(); err != nil {
		t.Fatalf("close recovery log: %v", err)
	}

	// Switch decisions and confirmation watermarks were persisted.
	recCheck, err := OpenRecoveryLog(filepath.Join(dir, "recovery.jsonl"))
	if err != nil {
		t.Fatalf("reopen for verification: %v", err)
	}
	if got := len(recCheck.Decisions()); got != 2 {
		t.Fatalf("both confirmed upgrade decisions must be persisted, got %d", got)
	}
	if wm := recCheck.Watermark("a", "s1"); wm != 2 {
		t.Fatalf("confirmation watermark must reach the last pre-shutdown switch, got %d", wm)
	}
	if err := recCheck.Close(); err != nil {
		t.Fatalf("close verification log: %v", err)
	}

	// Restart after consecutive upgrades: full history, no reset.
	rec2 := openTestRecovery(t, dir)
	mgr2 := testManager(t, []Manifest{streamManifest("a", "s1")},
		Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Recovery: rec2})
	subs, events, err := mgr2.DumpSubscriptions("a")
	if err != nil {
		t.Fatalf("DumpSubscriptions after consecutive-upgrade restart: %v", err)
	}
	if len(subs) != 1 || subs[0].ID != "s1" {
		t.Fatalf("subscription must survive consecutive upgrades and restart: %+v", subs)
	}
	if len(events) != 3 {
		t.Fatalf("all confirmed events must survive consecutive upgrades and restart: %+v", events)
	}
	for _, id := range []string{"e1", "e2", "e3"} {
		out := streamCall(t, mgr2, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": id, "pos": map[string]uint64{"e1": 1, "e2": 2, "e3": 3}[id]})
		if out["ok"] != "duplicate" {
			t.Fatalf("event %s confirmed before restart must dedupe: %+v", id, out)
		}
	}
	// Every generation's last responsible version is still on record.
	for session, want := range map[string]string{first: "3.1.4-test", second: "2.0.0", third: "3.0.0"} {
		owner, ok := mgr2.SessionOwner(session)
		if !ok || owner.Version != want || owner.Active {
			t.Fatalf("session %s must be retired as %s after restart: %+v", session[:8], want, owner)
		}
	}
	if err := rec2.Close(); err != nil {
		t.Fatalf("close second recovery log: %v", err)
	}
}
