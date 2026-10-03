package host

import (
	"strings"
	"testing"
	"time"
)

// subManifest builds a helper-extension manifest with the emit
// capability (plus any extra caps) and the given declared subscriptions.
func subManifest(name string, extraCaps []string, subs ...string) Manifest {
	return subManifestCap(name, "emit", extraCaps, subs...)
}

// subManifestCap is subManifest with a custom emitter capability name,
// for extensions that coexist with another emitter in one manager.
func subManifestCap(name, emitCap string, extraCaps []string, subs ...string) Manifest {
	mf := helperManifest(name, append([]string{emitCap}, extraCaps...), nil)
	if len(subs) > 0 {
		mf.Env = append(mf.Env, "EXTSYS_TEST_SUBS="+strings.Join(subs, ","))
	}
	return mf
}

func emitEvents(t *testing.T, m *Manager, sub string, count int) {
	t.Helper()
	emitEventsCap(t, m, "emit", sub, count)
}

func emitEventsCap(t *testing.T, m *Manager, cap, sub string, count int) {
	t.Helper()
	if _, err := m.Call(cap, map[string]any{"sub": sub, "count": count}); err != nil {
		t.Fatalf("emit %s via %s: %v", sub, cap, err)
	}
}

func getSub(t *testing.T, m *Manager, ext, sub string) SubscriptionStatus {
	t.Helper()
	for _, st := range m.Subscriptions(ext) {
		if st.Name == sub {
			return st
		}
	}
	t.Fatalf("no subscription %s/%s in ledger", ext, sub)
	return SubscriptionStatus{}
}

func waitSub(t *testing.T, m *Manager, ext, sub string, n int) SubscriptionStatus {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		found := false
		for _, st := range m.Subscriptions(ext) {
			if st.Name == sub {
				found = true
				if len(st.Events) >= n {
					return st
				}
			}
		}
		_ = found
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("subscription %s/%s did not deliver %d events in time", ext, sub, n)
	return SubscriptionStatus{}
}

func ackAll(t *testing.T, m *Manager, ext, sub string) {
	t.Helper()
	for _, ev := range getSub(t, m, ext, sub).Events {
		if !ev.Acked {
			if err := m.AckEvent(ext, sub, ev.ID); err != nil {
				t.Fatalf("ack %s/%s event %d: %v", ext, sub, ev.ID, err)
			}
		}
	}
}

func waitDead(t *testing.T, m *Manager, name string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && m.Alive(name) {
		time.Sleep(20 * time.Millisecond)
	}
	if m.Alive(name) {
		t.Fatalf("%s should be dead", name)
	}
}

func crash(t *testing.T, m *Manager) {
	t.Helper()
	_, _ = m.Call("panic", nil)
	waitDead(t, m, "a")
}

// The upgrade preview must show the subscription scope, per-event
// progress and whether the candidate can take over; a fully confirmed
// stream is handed over automatically and resumes exactly after the
// last confirmed event.
func TestSubscriptionPreviewAndHandoff(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{subManifest("a", nil, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 5)
	waitSub(t, mgr, "a", "ticks", 5)
	ackAll(t, mgr, "a", "ticks")
	oldSession := mgr.Session("a")

	candidate := withEnv(subManifest("a", nil, "ticks"), "EXTSYS_TEST_VER=2.0.0")
	plan, err := mgr.PrepareUpgrade([]Manifest{candidate})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if len(plan.Sessions) != 1 || len(plan.Sessions[0].Subs) != 1 {
		t.Fatalf("preview must show the subscription: %+v", plan.Sessions)
	}
	sp := plan.Sessions[0].Subs[0]
	if sp.Name != "ticks" || sp.NextID != 6 || sp.AckedUpTo != 5 {
		t.Fatalf("preview must show scope and progress: %+v", sp)
	}
	if len(sp.Pending) != 0 || len(sp.Uncertain) != 0 || !sp.CanResume || sp.NeedsDecision() {
		t.Fatalf("fully confirmed, resumable subscription must need no decision: %+v", sp)
	}

	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	newSession := mgr.Session("a")

	// The stream resumes after the last confirmed event, and new
	// confirmations belong to the new version.
	emitEvents(t, mgr, "ticks", 2)
	st := waitSub(t, mgr, "a", "ticks", 7)
	if st.Events[5].ID != 6 || st.Events[6].ID != 7 {
		t.Fatalf("stream must continue at 6, not replay: %+v", st.Events)
	}
	ackAll(t, mgr, "a", "ticks")

	acks := journal.AckRecords()
	if len(acks) != 7 {
		t.Fatalf("expected 7 confirmations, got %d", len(acks))
	}
	for i, a := range acks {
		wantVer, wantSess := "3.1.4-test", oldSession
		if i >= 5 {
			wantVer, wantSess = "2.0.0", newSession
		}
		if a.Version != wantVer || a.Session != wantSess {
			t.Fatalf("event %d confirmed by %s/%s, want %s/%s", a.EventID, a.Version, a.Session, wantVer, wantSess)
		}
	}
	st = getSub(t, mgr, "a", "ticks")
	if st.AckedUpTo != 7 || !st.Active {
		t.Fatalf("ledger must be fully confirmed and active: %+v", st)
	}
	owner, ok := mgr.SessionOwner(newSession)
	if !ok || !owner.Active || owner.Version != "2.0.0" {
		t.Fatalf("new session must own the extension: %+v", owner)
	}
}

// Events the old version delivered but had not yet confirmed are still
// confirmable after cutover — attributed to the old version. When the
// new version re-delivers the same events (it resumed at the last
// confirmed position), the duplicates are dropped and can never
// overwrite the confirmed records.
func TestSubscriptionLateConfirmationAndDuplicateDelivery(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{subManifest("a", nil, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 5)
	waitSub(t, mgr, "a", "ticks", 5)
	for _, id := range []uint64{1, 2, 3} {
		if err := mgr.AckEvent("a", "ticks", id); err != nil {
			t.Fatalf("ack %d: %v", id, err)
		}
	}
	oldSession := mgr.Session("a")

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", nil, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	sp := plan.Sessions[0].Subs[0]
	if len(sp.Pending) != 2 || sp.Pending[0] != 4 || sp.Pending[1] != 5 {
		t.Fatalf("preview must list the unconfirmed events: %+v", sp)
	}
	if sp.NeedsDecision() {
		t.Fatalf("live-owned unconfirmed events are safe to hand over: %+v", sp)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}

	// The old version's deliveries are confirmed after the cutover;
	// the confirmations belong to the old version and are not lost.
	for _, id := range []uint64{4, 5} {
		if err := mgr.AckEvent("a", "ticks", id); err != nil {
			t.Fatalf("late ack %d: %v", id, err)
		}
	}
	// The new version resumed at 4 and re-delivers 4 and 5: duplicates
	// must be dropped without touching the confirmed records.
	emitEvents(t, mgr, "ticks", 2)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && getSub(t, mgr, "a", "ticks").Duplicates < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	st := getSub(t, mgr, "a", "ticks")
	if st.Duplicates != 2 {
		t.Fatalf("expected 2 dropped duplicates, got %+v", st)
	}
	if len(st.Events) != 5 || st.AckedUpTo != 5 {
		t.Fatalf("duplicates must not add or change events: %+v", st)
	}
	for _, ev := range st.Events {
		if !ev.Acked || ev.Version != "3.1.4-test" || ev.Session != oldSession {
			t.Fatalf("confirmed events must keep the old version as owner: %+v", ev)
		}
	}
	if err := mgr.AckEvent("a", "ticks", 4); err == nil {
		t.Fatal("double confirmation must be rejected")
	}

	// The next genuinely new event is confirmed under the new version.
	emitEvents(t, mgr, "ticks", 1)
	waitSub(t, mgr, "a", "ticks", 6)
	if err := mgr.AckEvent("a", "ticks", 6); err != nil {
		t.Fatalf("ack 6: %v", err)
	}
	acks := journal.AckRecords()
	if len(acks) != 6 {
		t.Fatalf("expected 6 confirmations, got %d", len(acks))
	}
	last := acks[len(acks)-1]
	if last.EventID != 6 || last.Version != "2.0.0" || last.Session != mgr.Session("a") {
		t.Fatalf("wrong owner for event 6: %+v", last)
	}
}

// When the session that owned unconfirmed events dies (crash +
// restart), the host can no longer safely determine that part of the
// stream's progress: the preview must show the uncertain range and the
// upgrade must wait for an explicit user decision. Keeping the old
// version cancels the upgrade; the original subscription and all other
// sessions are unaffected.
func TestSubscriptionUncertainProgressRequiresDecision(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{
		subManifest("a", []string{"panic"}, "ticks"),
		subManifestCap("b", "emitb", nil, "beats"),
	}, cfg)

	emitEvents(t, mgr, "ticks", 5)
	waitSub(t, mgr, "a", "ticks", 5)
	for _, id := range []uint64{1, 2, 3} {
		if err := mgr.AckEvent("a", "ticks", id); err != nil {
			t.Fatalf("ack %d: %v", id, err)
		}
	}
	// A second session with its own subscription, to prove isolation.
	emitEventsCap(t, mgr, "emitb", "beats", 3)
	waitSub(t, mgr, "b", "beats", 3)
	if err := mgr.AckEvent("b", "beats", 1); err != nil {
		t.Fatalf("ack beats 1: %v", err)
	}

	// Interrupt: the owning session dies with events 4 and 5
	// unconfirmed, then the extension is restarted.
	crash(t, mgr)
	if err := mgr.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	restarted := mgr.Session("a")

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", []string{"panic"}, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	sp := plan.Sessions[0].Subs[0]
	if len(sp.Uncertain) != 2 || sp.Uncertain[0] != 4 || sp.Uncertain[1] != 5 {
		t.Fatalf("preview must show the uncertain range: %+v", sp)
	}
	if !sp.NeedsDecision() {
		t.Fatalf("uncertain progress must demand a decision: %+v", sp)
	}

	// No decision, no upgrade.
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err == nil {
		t.Fatal("confirm without a decision for uncertain progress must fail")
	}
	if mgr.Session("a") != restarted {
		t.Fatal("plan must stay open after a rejected confirmation")
	}

	// The user chooses to keep the old version: the upgrade is
	// cancelled, the original subscription continues.
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{
		Subs: map[string]Disposition{"a/ticks": DecideKeepOld},
	}); err == nil {
		t.Fatal("keep_old cancels the upgrade")
	}
	if mgr.Session("a") != restarted {
		t.Fatal("keep_old must retain the running session")
	}
	owner, ok := mgr.SessionOwner(restarted)
	if !ok || !owner.Active {
		t.Fatalf("restarted session must remain the active owner: %+v", owner)
	}
	if countEvent(journal, SwitchCutover) != 0 {
		t.Fatalf("cancelled upgrade must not cut over: %v", switchEvents(journal))
	}

	// The original subscription keeps flowing on the retained version;
	// re-delivery of 4 and 5 under the live session resolves the
	// uncertainty.
	emitEvents(t, mgr, "ticks", 2)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		st := getSub(t, mgr, "a", "ticks")
		if st.Events[3].Session == restarted && st.Events[4].Session == restarted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	st := getSub(t, mgr, "a", "ticks")
	if st.Events[3].Session != restarted || st.Events[4].Session != restarted {
		t.Fatalf("re-delivery must re-own events 4 and 5: %+v", st.Events)
	}

	// The other session was never touched.
	stB := getSub(t, mgr, "b", "beats")
	if len(stB.Events) != 3 || !stB.Events[0].Acked || stB.Events[1].Acked {
		t.Fatalf("other session's ledger must be unaffected: %+v", stB)
	}
	emitEventsCap(t, mgr, "emitb", "beats", 1)
	waitSub(t, mgr, "b", "beats", 4)

	// With the uncertainty resolved, the upgrade proceeds without any
	// subscription decision.
	ackAll(t, mgr, "a", "ticks")
	plan2, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", []string{"panic"}, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade 2: %v", err)
	}
	if plan2.Sessions[0].Subs[0].NeedsDecision() {
		t.Fatalf("resolved progress must not demand a decision: %+v", plan2.Sessions[0].Subs[0])
	}
	if err := mgr.ConfirmUpgrade(plan2.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade 2: %v", err)
	}
	if got := mgr.Session("a"); got == restarted || got == "" {
		t.Fatalf("second upgrade must activate a fresh session: %q", got)
	}
}

// The alternative explicit decision for uncertain progress: stop the
// subscription. The upgrade then proceeds, the stream is stopped on
// both builds, and the ledger records it as inactive.
func TestSubscriptionStopSubDecision(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{subManifest("a", []string{"panic"}, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 4)
	waitSub(t, mgr, "a", "ticks", 4)
	if err := mgr.AckEvent("a", "ticks", 1); err != nil {
		t.Fatalf("ack 1: %v", err)
	}
	crash(t, mgr)
	if err := mgr.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", []string{"panic"}, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if !plan.Sessions[0].Subs[0].NeedsDecision() {
		t.Fatalf("orphaned unconfirmed events must demand a decision: %+v", plan.Sessions[0].Subs[0])
	}
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{
		Subs: map[string]Disposition{"a/ticks": DecideStopSub},
	}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}

	st := getSub(t, mgr, "a", "ticks")
	if st.Active {
		t.Fatalf("stopped subscription must be inactive: %+v", st)
	}
	if _, err := mgr.Call("emit", map[string]any{"sub": "ticks", "count": 1}); err == nil {
		t.Fatal("emitting on a stopped subscription must fail")
	}
	// Previously confirmed events stay confirmed under the old owner.
	if st.Events[0].Version != "3.1.4-test" || !st.Events[0].Acked {
		t.Fatalf("confirmed history must survive stop_sub: %+v", st.Events[0])
	}
}

// Rollback must verify the old build can explain the subscription
// progress the new version accumulated: a declaring build resumes
// exactly after the last confirmed event (never replaying confirmed
// events); a build that cannot explain the stream is reported and the
// subscription is retired instead of silently restarted.
func TestSubscriptionRollbackChecksProgress(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{subManifest("a", nil, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 5)
	waitSub(t, mgr, "a", "ticks", 5)
	ackAll(t, mgr, "a", "ticks")

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", nil, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	emitEvents(t, mgr, "ticks", 3)
	waitSub(t, mgr, "a", "ticks", 8)
	ackAll(t, mgr, "a", "ticks")

	// Roll back to a build that declares the subscription.
	report, err := mgr.Rollback("a", subManifest("a", nil, "ticks"))
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if len(report.AcceptedSubs) != 1 || report.AcceptedSubs[0] != "ticks" || !report.SubsFullyResumed {
		t.Fatalf("old build must resume the subscription: %+v", report)
	}
	emitEvents(t, mgr, "ticks", 2)
	st := waitSub(t, mgr, "a", "ticks", 10)
	if st.Events[8].ID != 9 || st.Events[9].ID != 10 {
		t.Fatalf("rollback must resume after the last confirmed event, not replay: %+v", st.Events)
	}
	if st.Duplicates != 0 {
		t.Fatalf("rollback must not re-deliver confirmed events: %+v", st)
	}

	// Roll back to a build that cannot explain the stream.
	report2, err := mgr.Rollback("a", withEnv(subManifest("a", nil), "EXTSYS_TEST_VER=0.9.0"))
	if err != nil {
		t.Fatalf("Rollback 2: %v", err)
	}
	if len(report2.RejectedSubs) != 1 || report2.RejectedSubs[0] != "ticks" || report2.SubsFullyResumed {
		t.Fatalf("unexplainable subscription must be reported: %+v", report2)
	}
	if st := getSub(t, mgr, "a", "ticks"); st.Active {
		t.Fatalf("unresumable subscription must be retired, not silently restarted: %+v", st)
	}
}

// The non-interactive Upgrade path must not bypass subscription
// decisions: it refuses when a subscription cannot be resumed by the
// candidate, and hands a fully confirmed stream over automatically.
func TestAutoUpgradeRefusedWhenSubscriptionUncertain(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: NewMemoryJournal()}
	mgr := testManager(t, []Manifest{subManifest("a", nil, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 3)
	waitSub(t, mgr, "a", "ticks", 3)
	ackAll(t, mgr, "a", "ticks")
	oldSession := mgr.Session("a")

	// Candidate cannot resume the subscription: automatic upgrade must
	// refuse instead of dropping the stream.
	noSub := withEnv(subManifest("a", nil), "EXTSYS_TEST_VER=2.0.0")
	if err := mgr.Upgrade([]Manifest{noSub}); err == nil {
		t.Fatal("auto upgrade must refuse when the candidate cannot resume the subscription")
	}
	if mgr.Session("a") != oldSession {
		t.Fatal("refused upgrade must keep the old session serving")
	}
	emitEvents(t, mgr, "ticks", 1)
	waitSub(t, mgr, "a", "ticks", 4)
	ackAll(t, mgr, "a", "ticks")

	// Fully confirmed and resumable: the automatic handoff is safe.
	good := withEnv(subManifest("a", nil, "ticks"), "EXTSYS_TEST_VER=2.0.0")
	if err := mgr.Upgrade([]Manifest{good}); err != nil {
		t.Fatalf("auto upgrade with a clean stream: %v", err)
	}
	emitEvents(t, mgr, "ticks", 2)
	st := waitSub(t, mgr, "a", "ticks", 6)
	if st.Events[4].ID != 5 || st.Events[5].ID != 6 {
		t.Fatalf("auto handoff must resume after the last confirmed event: %+v", st.Events)
	}
}

// Interrupt and restart around every phase: the prepared upgrade of a
// crashed old session fails cleanly without touching the restarted
// incarnation, and a rollback after an interrupted switch still resumes
// the subscription at the confirmed position.
func TestSubscriptionInterruptAndRestartAcrossPhases(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{subManifest("a", []string{"panic"}, "ticks")}, cfg)

	emitEvents(t, mgr, "ticks", 3)
	waitSub(t, mgr, "a", "ticks", 3)
	ackAll(t, mgr, "a", "ticks")

	// --- Interrupted during preparation ----------------------------
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", []string{"panic"}, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	crash(t, mgr)
	if err := mgr.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	restarted := mgr.Session("a")
	// The prepared plan referenced the dead session: confirming must
	// fail and tear down only the candidates.
	if err := mgr.ConfirmUpgrade(plan.ID, ConfirmDecisions{}); err == nil {
		t.Fatal("confirming a plan whose old session died must fail")
	}
	if mgr.Session("a") != restarted {
		t.Fatal("failed confirmation must not replace the restarted session")
	}
	owner, ok := mgr.SessionOwner(restarted)
	if !ok || !owner.Active {
		t.Fatalf("restarted session must remain responsible: %+v", owner)
	}
	if countEvent(journal, SwitchCutover) != 0 || countEvent(journal, SwitchActivated) != 0 {
		t.Fatalf("interrupted upgrade must not activate: %v", switchEvents(journal))
	}
	// The ledger survived the interruption; the stream continues.
	emitEvents(t, mgr, "ticks", 1)
	waitSub(t, mgr, "a", "ticks", 4)
	ackAll(t, mgr, "a", "ticks")

	// --- Switch, then interrupt before rollback --------------------
	plan2, err := mgr.PrepareUpgrade([]Manifest{withEnv(subManifest("a", []string{"panic"}, "ticks"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade 2: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan2.ID, ConfirmDecisions{}); err != nil {
		t.Fatalf("ConfirmUpgrade 2: %v", err)
	}
	emitEvents(t, mgr, "ticks", 2)
	waitSub(t, mgr, "a", "ticks", 6)
	ackAll(t, mgr, "a", "ticks")

	crash(t, mgr)
	if err := mgr.Restart("a"); err != nil {
		t.Fatalf("Restart 2: %v", err)
	}
	// Rollback after the interruption: the old build resumes the
	// subscription exactly after the last confirmed event.
	report, err := mgr.Rollback("a", subManifest("a", []string{"panic"}, "ticks"))
	if err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if !report.SubsFullyResumed || len(report.AcceptedSubs) != 1 {
		t.Fatalf("rollback must resume the subscription: %+v", report)
	}
	emitEvents(t, mgr, "ticks", 1)
	st := waitSub(t, mgr, "a", "ticks", 7)
	if st.Events[6].ID != 7 || st.Duplicates != 0 {
		t.Fatalf("rollback must not replay confirmed events: %+v", st)
	}
	ackAll(t, mgr, "a", "ticks")
	if st := getSub(t, mgr, "a", "ticks"); st.AckedUpTo != 7 {
		t.Fatalf("final ledger must be fully confirmed: %+v", st)
	}
	finalOwner, ok := mgr.SessionOwner(mgr.Session("a"))
	if !ok || !finalOwner.Active {
		t.Fatalf("final session must be actively owned: %+v", finalOwner)
	}
}
