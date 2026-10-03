package host

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"extsys/internal/protocol"
)

func statefulManifest(name string, stateKeys ...string) Manifest {
	mf := helperManifest(name, []string{"bump"}, nil)
	mf.Env = append(mf.Env, "EXTSYS_TEST_STATE_KEYS="+strings.Join(stateKeys, ","))
	return mf
}

func bump(t *testing.T, m *Manager, key string, delta int) {
	t.Helper()
	if _, err := m.Call("bump", map[string]any{"key": key, "delta": delta}); err != nil {
		t.Fatalf("bump %s: %v", key, err)
	}
}

func stateValue(t *testing.T, items []protocol.StateItem, key string) (int, bool) {
	t.Helper()
	for _, it := range items {
		if it.Key == key {
			var n int
			if err := json.Unmarshal(it.Value, &n); err != nil {
				t.Fatalf("state item %s not an int: %v", key, err)
			}
			return n, true
		}
	}
	return 0, false
}

func TestUpgradeStatePreviewAndConfirm(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{
		statefulManifest("a", "counter", "legacy"),
	}, cfg)

	bump(t, mgr, "counter", 2)
	bump(t, mgr, "legacy", 5)
	oldSession := mgr.Session("a")

	// Candidate build no longer understands "legacy".
	candidate := withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")
	plan, err := mgr.PrepareUpgrade([]Manifest{candidate})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if len(plan.Sessions) != 1 {
		t.Fatalf("expected one session preview, got %+v", plan.Sessions)
	}
	preview := plan.Sessions[0]
	if preview.OldSession != oldSession || preview.OldVersion != "3.1.4-test" || preview.NewVersion != "2.0.0" {
		t.Fatalf("preview must identify both versions and the old session: %+v", preview)
	}
	if len(preview.Items) != 2 {
		t.Fatalf("old session should hold 2 state items, got %+v", preview.Items)
	}
	if len(preview.Accepted) != 1 || preview.Accepted[0] != "counter" {
		t.Fatalf("candidate should accept only counter: %+v", preview.Accepted)
	}
	if len(preview.Rejected) != 1 || preview.Rejected[0] != "legacy" {
		t.Fatalf("candidate should reject legacy: %+v", preview.Rejected)
	}

	// Incompatible items demand an explicit decision.
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err == nil {
		t.Fatal("confirm without a decision for incompatible items must fail")
	}
	// The failed attempt must not have activated anything.
	if mgr.Session("a") != oldSession {
		t.Fatal("plan must still be open after a rejected confirmation")
	}

	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{State: map[string]Disposition{"a": DecideMigrate}}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	newSession := mgr.Session("a")
	if newSession == oldSession || newSession == "" {
		t.Fatalf("session must rotate on upgrade: %q -> %q", oldSession, newSession)
	}

	// Accepted state migrated; rejected state dropped.
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "counter"); !ok || v != 2 {
		t.Fatalf("counter must migrate with value 2, got %d ok=%v (%+v)", v, ok, items)
	}
	if _, ok := stateValue(t, items, "legacy"); ok {
		t.Fatalf("legacy must not survive migration: %+v", items)
	}

	// Audit trail distinguishes the phases in order.
	events := switchEvents(journal)
	want := []string{SwitchValidate, SwitchPrepared, SwitchStateCheck, SwitchConfirmed, SwitchCutover, SwitchDrained, SwitchActivated}
	pos := -1
	for _, w := range want {
		found := -1
		for i := pos + 1; i < len(events); i++ {
			if events[i] == w {
				found = i
				break
			}
		}
		if found == -1 {
			t.Fatalf("event %s missing after position %d in %v", w, pos, events)
		}
		pos = found
	}

	// Ownership: the old session is retired, the new one responsible.
	oldOwner, ok := mgr.SessionOwner(oldSession)
	if !ok || oldOwner.Active || oldOwner.Version != "3.1.4-test" {
		t.Fatalf("old session must be retired: %+v", oldOwner)
	}
	newOwner, ok := mgr.SessionOwner(newSession)
	if !ok || !newOwner.Active || newOwner.Version != "2.0.0" {
		t.Fatalf("new session must own the extension: %+v", newOwner)
	}
}

func TestUpgradeStateChangesDuringPreparationSurvive(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter")}, cfg)

	bump(t, mgr, "counter", 1)
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	// The old session keeps producing state while the plan is open.
	bump(t, mgr, "counter", 10)
	bump(t, mgr, "counter", 100)

	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "counter"); !ok || v != 111 {
		t.Fatalf("changes confirmed during preparation must not be lost: got %d ok=%v", v, ok)
	}
}

func TestUpgradeCancelRetainsOldVersionAndState(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter")}, cfg)

	bump(t, mgr, "counter", 7)
	oldSession := mgr.Session("a")

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.CancelUpgrade(plan.ID, "not today"); err != nil {
		t.Fatalf("CancelUpgrade: %v", err)
	}
	if mgr.Session("a") != oldSession {
		t.Fatal("cancelled upgrade must keep the old session serving")
	}
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "counter"); !ok || v != 7 {
		t.Fatalf("old session state must survive cancellation: got %d ok=%v", v, ok)
	}
	owner, ok := mgr.SessionOwner(oldSession)
	if !ok || !owner.Active {
		t.Fatalf("old session must remain the active owner: %+v", owner)
	}
	if countEvent(journal, SwitchCutover) != 0 || countEvent(journal, SwitchActivated) != 0 {
		t.Fatalf("cancelled upgrade must not activate: %v", switchEvents(journal))
	}
	if countEvent(journal, SwitchRollback) == 0 {
		t.Fatalf("cancelled candidate teardown must be recorded: %v", switchEvents(journal))
	}
}

func TestUpgradeDiscardDropsSessionState(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter", "legacy")}, cfg)

	bump(t, mgr, "counter", 3)
	bump(t, mgr, "legacy", 4)

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{State: map[string]Disposition{"a": DecideDiscard}}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("discard must drop all session state, got %+v", items)
	}
}

func TestRollbackReportsStateTheOldBuildCannotReceive(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter")}, cfg)

	bump(t, mgr, "counter", 1)
	// v2 understands a new key the old build never declared.
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter", "v2only"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	bump(t, mgr, "counter", 10)
	bump(t, mgr, "v2only", 42)

	report, err := mgr.Rollback("a", statefulManifest("a", "counter"))
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if report.StateFullyAccepted {
		t.Fatalf("old build cannot receive v2only; rollback must not claim full restore: %+v", report)
	}
	if len(report.Rejected) != 1 || report.Rejected[0] != "v2only" {
		t.Fatalf("rollback must list the unreceivable keys: %+v", report.Rejected)
	}
	if report.FromVersion != "2.0.0" {
		t.Fatalf("report must name the version being rolled back: %+v", report)
	}
	// The old build is back and received what it can interpret.
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "counter"); !ok || v != 11 {
		t.Fatalf("counter must be restored to the old build: got %d ok=%v", v, ok)
	}
	if _, ok := stateValue(t, items, "v2only"); ok {
		t.Fatal("v2only must not be forced onto the old build")
	}
	owner, ok := mgr.SessionOwner(mgr.Session("a"))
	if !ok || !owner.Active {
		t.Fatalf("rolled-back session must be actively owned: %+v", owner)
	}
	if countEvent(journal, SwitchActivated) == 0 {
		t.Fatal("rollback must record its activation")
	}
}

func TestSessionOwnershipAcrossFailureAndConsecutiveUpgrades(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter")}, cfg)
	bump(t, mgr, "counter", 1)
	first := mgr.Session("a")

	// Dying candidate: old session stays responsible.
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_EXIT=1")})
	if err == nil {
		t.Fatal("expected preparation failure for a dying candidate")
	}
	if plan != nil {
		t.Fatal("no plan should remain after a failed preparation")
	}
	owner, ok := mgr.SessionOwner(first)
	if !ok || !owner.Active {
		t.Fatalf("old session must remain responsible after failed preparation: %+v", owner)
	}

	// Two consecutive upgrades: every generation is answerable.
	plan2, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan2.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	second := mgr.Session("a")
	plan3, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=3.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan3.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	third := mgr.Session("a")

	for session, want := range map[string]struct {
		version string
		active  bool
	}{
		first:  {"3.1.4-test", false},
		second: {"2.0.0", false},
		third:  {"3.0.0", true},
	} {
		owner, ok := mgr.SessionOwner(session)
		if !ok {
			t.Fatalf("no ownership record for session %q", session)
		}
		if owner.Version != want.version || owner.Active != want.active {
			t.Fatalf("session %q: got %+v", session, owner)
		}
	}

	// State survived both migrations.
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "counter"); !ok || v != 1 {
		t.Fatalf("counter must survive consecutive upgrades: got %d ok=%v", v, ok)
	}
}

func TestShutdownDuringPreparedUpgradeAbortsPlan(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{statefulManifest("a", "counter")}, cfg)
	bump(t, mgr, "counter", 5)

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(statefulManifest("a", "counter"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	mgr.Shutdown()

	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err == nil {
		t.Fatal("confirm after shutdown must fail")
	}
	// The old session is retired by the shutdown itself, not by the
	// aborted upgrade.
	owner, ok := mgr.SessionOwner(mgr.Session("a"))
	if !ok || owner.Active {
		t.Fatalf("session must be inactive after shutdown: %+v", owner)
	}
}
