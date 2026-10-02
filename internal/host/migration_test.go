package host

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// statefulManifest returns a manifest for the helper extension with
// the stateful "bump" capability and state hooks enabled.
func statefulManifest(name string, kv ...string) Manifest {
	mf := helperManifest(name, []string{"bump"}, nil)
	return withEnv(mf, append([]string{"EXTSYS_TEST_STATE=1"}, kv...)...)
}

func bump(t *testing.T, mgr *Manager) int {
	t.Helper()
	raw, err := mgr.Call("bump", nil)
	if err != nil {
		t.Fatalf("bump: %v", err)
	}
	var out struct {
		Counter int `json:"counter"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("decode bump result: %v", err)
	}
	return out.Counter
}

func findCheck(t *testing.T, plan *UpgradePlan, ext string) SessionCheck {
	t.Helper()
	for _, c := range plan.Checks() {
		if c.Extension == ext {
			return c
		}
	}
	t.Fatalf("no state check for extension %s", ext)
	return SessionCheck{}
}

func TestPlanPreviewAndCommitMigratesState(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	if got := bump(t, mgr); got != 1 {
		t.Fatalf("counter should start at 1, got %d", got)
	}
	bump(t, mgr)
	oldSession := mgr.Session("a")

	plan, err := mgr.PlanUpgrade([]Manifest{statefulManifest("a", "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	check := findCheck(t, plan, "a")
	if check.Session != oldSession || check.OldVersion != "3.1.4-test" || check.NewVersion != "2.0.0" {
		t.Fatalf("check must identify old and new versions: %+v", check)
	}
	if check.HasRejected() {
		t.Fatalf("compatible candidate must accept every item: %+v", check.Items)
	}
	if len(check.Items) != 2 {
		t.Fatalf("expected counter+legacy state items, got %+v", check.Items)
	}

	if err := plan.Commit(map[string]SessionDecision{}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The counter state must have been carried to the new build.
	if got := bump(t, mgr); got != 3 {
		t.Fatalf("state lost in migration: counter after upgrade = %d, want 3", got)
	}

	// Audit trail distinguishes state check, user confirmation,
	// activation.
	for _, want := range []string{SwitchStateCheck, SwitchConfirmed, SwitchCutover, SwitchActivated} {
		if countEvent(journal, want) == 0 {
			t.Fatalf("audit trail missing %q: %v", want, switchEvents(journal))
		}
	}

	// Ownership ledger: old session retired, new session active.
	owner, ok := mgr.SessionOwner("a")
	if !ok || owner.Version != "2.0.0" || owner.Session != mgr.Session("a") {
		t.Fatalf("new version must own the session: %+v", owner)
	}
	var sawRetired bool
	for _, e := range mgr.SessionOwnership("a") {
		if e.Session == oldSession && e.Status == "retired" {
			sawRetired = true
		}
	}
	if !sawRetired {
		t.Fatalf("old session must be recorded retired: %+v", mgr.SessionOwnership("a"))
	}
}

func TestPlanIncompatibleItemsRequireExplicitDecision(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	bump(t, mgr)
	oldSession := mgr.Session("a")

	candidate := statefulManifest("a", "EXTSYS_TEST_VER=2.0.0", "EXTSYS_TEST_REJECT=legacy")
	plan, err := mgr.PlanUpgrade([]Manifest{candidate})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	check := findCheck(t, plan, "a")
	if !check.HasRejected() {
		t.Fatalf("candidate must reject the legacy item: %+v", check.Items)
	}
	var rejectedKey string
	for _, it := range check.Items {
		if !it.Accepted {
			rejectedKey = it.Key
			if it.Reason == "" {
				t.Fatalf("rejected item must carry a reason: %+v", it)
			}
		}
	}
	if rejectedKey != "legacy" {
		t.Fatalf("expected legacy to be rejected, got %q", rejectedKey)
	}

	// Committing without a decision for the affected session fails and
	// leaves the plan open.
	if err := plan.Commit(map[string]SessionDecision{}); err == nil {
		t.Fatal("commit without a decision must fail when items are incompatible")
	}
	if got := mgr.Session("a"); got != oldSession {
		t.Fatalf("undecided commit must not touch the running session")
	}

	// Explicit migrate decision drops the rejected item and proceeds.
	err = plan.Commit(map[string]SessionDecision{oldSession: DecisionMigrate})
	if err != nil {
		t.Fatalf("Commit with migrate decision: %v", err)
	}
	if got := bump(t, mgr); got != 2 {
		t.Fatalf("accepted state must survive migration: counter = %d, want 2", got)
	}
	if countEvent(journal, SwitchConfirmed) == 0 {
		t.Fatalf("user confirmation must be recorded: %v", switchEvents(journal))
	}
}

func TestPlanDecisionCancelRetainsOld(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	bump(t, mgr)
	oldSession := mgr.Session("a")

	candidate := statefulManifest("a", "EXTSYS_TEST_VER=2.0.0", "EXTSYS_TEST_REJECT=legacy")
	plan, err := mgr.PlanUpgrade([]Manifest{candidate})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	err = plan.Commit(map[string]SessionDecision{oldSession: DecisionCancel})
	if err == nil {
		t.Fatal("cancel decision must abort the upgrade")
	}
	if got := mgr.Session("a"); got != oldSession {
		t.Fatal("cancelled upgrade must keep the old session")
	}
	if got := bump(t, mgr); got != 2 {
		t.Fatalf("old version must keep serving with its state: counter = %d", got)
	}
	if countEvent(journal, SwitchCutover) != 0 || countEvent(journal, SwitchActivated) != 0 {
		t.Fatalf("cancelled upgrade must not cut over: %v", switchEvents(journal))
	}
	owner, ok := mgr.SessionOwner("a")
	if !ok || owner.Session != oldSession {
		t.Fatalf("old session must remain responsible: %+v", owner)
	}
	var retained bool
	for _, e := range mgr.SessionOwnership("a") {
		if e.Session == oldSession && e.Status == "retained" {
			retained = true
		}
	}
	if !retained {
		t.Fatalf("abort must record the old session as retained: %+v", mgr.SessionOwnership("a"))
	}
}

func TestAutomaticUpgradeRefusesIncompatibleState(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	bump(t, mgr)
	oldSession := mgr.Session("a")
	candidate := statefulManifest("a", "EXTSYS_TEST_VER=2.0.0", "EXTSYS_TEST_REJECT=legacy")
	if err := mgr.Upgrade([]Manifest{candidate}); err == nil {
		t.Fatal("automatic upgrade must refuse to drop incompatible state")
	}
	if got := mgr.Session("a"); got != oldSession {
		t.Fatal("refused upgrade must keep the old session")
	}
	if got := bump(t, mgr); got != 2 {
		t.Fatalf("old version must keep serving: counter = %d", got)
	}
}

func TestStateChangesDuringPreparationAreNotLost(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	bump(t, mgr) // counter = 1 before the plan
	plan, err := mgr.PlanUpgrade([]Manifest{statefulManifest("a", "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	// The old session keeps producing confirmed state changes while
	// the candidate is prepared.
	bump(t, mgr) // counter = 2
	bump(t, mgr) // counter = 3

	if err := plan.Commit(map[string]SessionDecision{}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if got := bump(t, mgr); got != 4 {
		t.Fatalf("changes during preparation were lost: counter = %d, want 4", got)
	}
}

func TestOwnershipAcrossConsecutiveUpgradesAndFailedCandidate(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	v1 := mgr.Session("a")
	if err := mgr.Upgrade([]Manifest{statefulManifest("a", "EXTSYS_TEST_VER=2.0.0")}); err != nil {
		t.Fatalf("first upgrade: %v", err)
	}
	v2 := mgr.Session("a")

	// A candidate that dies before the handshake must not change
	// ownership.
	dead := withEnv(helperManifest("a", []string{"bump"}, nil), "EXTSYS_TEST_EXIT=1")
	if err := mgr.Upgrade([]Manifest{dead}); err == nil {
		t.Fatal("expected failure for dying candidate")
	}
	owner, ok := mgr.SessionOwner("a")
	if !ok || owner.Session != v2 || owner.Version != "2.0.0" {
		t.Fatalf("failed candidate must not take ownership: %+v", owner)
	}

	if err := mgr.Upgrade([]Manifest{statefulManifest("a", "EXTSYS_TEST_VER=3.0.0")}); err != nil {
		t.Fatalf("second upgrade: %v", err)
	}
	v3 := mgr.Session("a")
	if v1 == v2 || v2 == v3 || v1 == v3 {
		t.Fatalf("sessions must be distinct across upgrades: %q %q %q", v1, v2, v3)
	}
	owner, ok = mgr.SessionOwner("a")
	if !ok || owner.Session != v3 || owner.Version != "3.0.0" {
		t.Fatalf("latest version must own the session: %+v", owner)
	}
	// Every generation is queryable: v1 retired, v2 retired (and once
	// retained after the failed candidate), v3 active.
	history := mgr.SessionOwnership("a")
	status := map[string][]string{}
	for _, e := range history {
		status[e.Session] = append(status[e.Session], e.Status)
	}
	if !contains(status[v1], "retired") || !contains(status[v2], "retired") ||
		!contains(status[v2], "retained") || !contains(status[v3], "active") {
		t.Fatalf("ownership history incomplete: %+v", history)
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func TestShutdownCancelsPendingPlan(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	oldSession := mgr.Session("a")
	plan, err := mgr.PlanUpgrade([]Manifest{statefulManifest("a", "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	mgr.Shutdown() // preparation stopped midway by host shutdown

	if err := plan.Commit(map[string]SessionDecision{}); err == nil {
		t.Fatal("commit after shutdown must fail")
	}
	if countEvent(journal, SwitchCutover) != 0 {
		t.Fatalf("shutdown during preparation must not cut over: %v", switchEvents(journal))
	}
	owner, ok := mgr.SessionOwner("a")
	if !ok || owner.Session != oldSession {
		t.Fatalf("old session must remain responsible after aborted preparation: %+v", owner)
	}
	var retained bool
	for _, e := range mgr.SessionOwnership("a") {
		if e.Session == oldSession && e.Status == "retained" &&
			strings.Contains(e.Detail, "host closed") {
			retained = true
		}
	}
	if !retained {
		t.Fatalf("abort by shutdown must be visible in the ledger: %+v", mgr.SessionOwnership("a"))
	}
}

func TestRollbackReportsWhetherOldBuildAcceptsNewState(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{statefulManifest("a")}, cfg)

	bump(t, mgr)
	// Upgrade to a build that cannot interpret the legacy item; the
	// user migrates, dropping it.
	plan, err := mgr.PlanUpgrade([]Manifest{
		statefulManifest("a", "EXTSYS_TEST_VER=2.0.0", "EXTSYS_TEST_REJECT=legacy"),
	})
	if err != nil {
		t.Fatalf("PlanUpgrade: %v", err)
	}
	decisions := map[string]SessionDecision{}
	for _, c := range plan.Checks() {
		decisions[c.Session] = DecisionMigrate
	}
	if err := plan.Commit(decisions); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	bump(t, mgr) // new build keeps producing state

	// Roll back to a build that accepts everything: state restored.
	rep, err := mgr.Rollback("a", statefulManifest("a", "EXTSYS_TEST_VER=1.0.0"))
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if !rep.StateRestored {
		t.Fatalf("compatible old build must fully accept the state: %+v", rep)
	}
	if rep.FromVersion != "2.0.0" || rep.ToVersion != "1.0.0" {
		t.Fatalf("report must name both versions: %+v", rep)
	}
	if got := bump(t, mgr); got != 3 {
		t.Fatalf("state must survive the rollback: counter = %d, want 3", got)
	}

	// Roll back to a build that cannot interpret the counter: the
	// report must say the state is NOT restored, even though the old
	// process runs again.
	rep, err = mgr.Rollback("a", statefulManifest("a", "EXTSYS_TEST_VER=0.9.0", "EXTSYS_TEST_REJECT=counter"))
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if rep.StateRestored {
		t.Fatalf("rollback dropping the counter must not claim state restored: %+v", rep)
	}
	if !strings.Contains(rep.Detail, "rejected") {
		t.Fatalf("report detail must explain the partial restore: %q", rep.Detail)
	}
	if got := bump(t, mgr); got != 1 {
		t.Fatalf("counter was not accepted by the old build: counter = %d, want 1", got)
	}
	owner, ok := mgr.SessionOwner("a")
	if !ok || owner.Version != "0.9.0" {
		t.Fatalf("rolled-back version must own the session: %+v", owner)
	}
}
