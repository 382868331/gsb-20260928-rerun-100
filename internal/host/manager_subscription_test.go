package host

import (
	"encoding/json"
	"testing"
	"time"
)

func streamManifest(name string, subscriptions ...string) Manifest {
	mf := helperManifest(name, []string{"stream_" + name}, nil)
	mf.Env = append(mf.Env,
		"EXTSYS_TEST_SUB_KEYS="+joinComma(subscriptions),
		"EXTSYS_TEST_STATE_KEYS=effects",
	)
	return mf
}

func joinComma(values []string) string {
	out := ""
	for i, v := range values {
		if i > 0 {
			out += ","
		}
		out += v
	}
	return out
}

func streamCall(t *testing.T, m *Manager, capability string, payload map[string]any) map[string]any {
	t.Helper()
	raw, err := m.Call(capability, payload)
	if err != nil {
		t.Fatalf("stream call: %v", err)
	}
	var out map[string]any
	if err := decodeJSON(raw, &out); err != nil {
		t.Fatalf("decode stream result: %v", err)
	}
	return out
}

func decodeJSON(raw []byte, out any) error {
	return json.Unmarshal(raw, out)
}

func eventPreview(plan *UpgradePlan, subID string) SubscriptionPreview {
	for _, s := range plan.Sessions {
		for _, p := range s.Subscriptions {
			if p.Scope.ID == subID {
				return p
			}
		}
	}
	return SubscriptionPreview{}
}

func TestUpgradeSubscriptionProgressRequiresDecision(t *testing.T) {
	mgr := testManager(t, []Manifest{streamManifest("a", "s1")},
		Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second})
	old := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual", "cursor": "0"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "report", "sub_id": "s1", "event": "e1", "pos": 1})

	candidate := withEnv(streamManifest("a", "s1"), "EXTSYS_TEST_VER=2.0.0")
	plan, err := mgr.PrepareUpgrade([]Manifest{candidate})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	p := eventPreview(plan, "s1")
	if p.SafeToContinue || len(p.InFlight) != 1 || p.InFlight[0] != "e1" || p.Uncertainty == "" {
		t.Fatalf("unconfirmed event must be shown as unsafe: %+v", p)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err == nil {
		t.Fatal("unconfirmed subscription cannot be auto-upgraded")
	}
	if mgr.Session("a") != old {
		t.Fatal("blocked confirmation must retain old owner")
	}
	if err := mgr.CancelUpgrade(plan.ID, "subscription unresolved"); err != nil {
		t.Fatalf("CancelUpgrade: %v", err)
	}
	if owner, ok := mgr.SessionOwner(old); !ok || !owner.Active {
		t.Fatalf("cancelled plan must leave original subscription owner active: %+v", owner)
	}

	// Re-open preparation after the old version has externally confirmed e1.
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	plan2, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("second PrepareUpgrade: %v", err)
	}
	p2 := eventPreview(plan2, "s1")
	if !p2.SafeToContinue || len(p2.InFlight) != 0 || len(p2.Confirmed) != 1 {
		t.Fatalf("confirmed subscription should be resumable: %+v", p2)
	}
	if err := mgr.ConfirmUpgrade(plan2.ID, nil); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	if out["ok"] != "duplicate" {
		t.Fatalf("new owner must treat redelivery as a duplicate: %+v", out)
	}
	items, err := mgr.DumpState("a")
	if err != nil {
		t.Fatalf("DumpState: %v", err)
	}
	if v, ok := stateValue(t, items, "effects"); !ok || v != 1 {
		t.Fatalf("confirmed effect must belong only to old version, got %d: %+v", v, items)
	}
}

func TestUpgradeLateOldConfirmationDoesNotOverwriteNewOwner(t *testing.T) {
	mgr := testManager(t, []Manifest{streamManifest("a", "s1"), streamManifest("b", "s2")},
		Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual", "cursor": "0"})
	oldA, oldB := mgr.Session("a"), mgr.Session("b")

	finished := make(chan map[string]any, 1)
	go func() {
		raw, err := mgr.Call("stream_a", map[string]any{
			"action": "slow_report", "sub_id": "s1", "event": "late", "pos": 7, "millis": 500,
		})
		if err != nil {
			t.Errorf("slow old confirmation: %v", err)
		}
		var out map[string]any
		if err := decodeJSON(raw, &out); err == nil {
			finished <- out
		}
	}()

	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	oldResult := <-finished
	if oldResult["ok"] != "reported" {
		t.Fatalf("late result must remain attributed to old call: %+v", oldResult)
	}
	if owner, ok := mgr.SessionOwner(oldA); !ok || owner.Active || owner.Version != "3.1.4-test" {
		t.Fatalf("old a session must be retired: %+v", owner)
	}
	if owner, ok := mgr.SessionOwner(oldB); !ok || !owner.Active {
		t.Fatalf("unrelated session b must remain active: %+v", owner)
	}
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "late", "pos": 7})
	if out["ok"] != "duplicate" {
		t.Fatalf("late old confirmation must become a new-owner dedupe marker: %+v", out)
	}
}

func TestRollbackRejectsSubscriptionProgressOldBuildCannotExplain(t *testing.T) {
	mgr := testManager(t, []Manifest{streamManifest("a", "s1")},
		Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual", "cursor": "0"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1", "newsub"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, nil); err != nil {
		t.Fatalf("ConfirmUpgrade: %v", err)
	}
	newSession := mgr.Session("a")
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "newsub", "source": "virtual"})

	report, err := mgr.Rollback("a", streamManifest("a", "s1"))
	if err == nil {
		t.Fatal("rollback over an unknown subscription must fail instead of replaying it")
	}
	if report == nil || report.SubscriptionSafe {
		t.Fatalf("unsafe rollback must report uncertain subscription scope: %+v", report)
	}
	if owner, ok := mgr.SessionOwner(newSession); !ok || !owner.Active || owner.Version != "2.0.0" {
		t.Fatalf("unsafe rollback must leave new version responsible: %+v", owner)
	}

	// A build that understands every subscription can roll back and
	// still uses the confirmed marker to prevent duplicate effects.
	safePrevious := withEnv(streamManifest("a", "s1", "newsub"), "EXTSYS_TEST_VER=1.9.0")
	report, err = mgr.Rollback("a", safePrevious)
	if err != nil {
		t.Fatalf("safe Rollback: %v report=%+v", err, report)
	}
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s1", "event": "e1", "pos": 1})
	if out["ok"] != "duplicate" {
		t.Fatalf("rollback must not replay confirmed e1: %+v", out)
	}
}

func TestStopSubscriptionDecisionDropsOnlyNamedScope(t *testing.T) {
	mgr := testManager(t, []Manifest{streamManifest("a", "s1", "s2")},
		Config{TaskTimeout: 2 * time.Second, ShutdownGrace: time.Second})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s1", "source": "virtual"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "save", "sub_id": "s2", "source": "virtual"})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "report", "sub_id": "s1", "event": "e1", "pos": 1})
	streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "e2", "pos": 2})
	plan, err := mgr.PrepareUpgrade([]Manifest{withEnv(streamManifest("a", "s1", "s2"), "EXTSYS_TEST_VER=2.0.0")})
	if err != nil {
		t.Fatalf("PrepareUpgrade: %v", err)
	}
	if err := mgr.ConfirmUpgrade(plan.ID, map[string]Disposition{"subscription:s1": DecideStopSubscription}); err != nil {
		t.Fatalf("ConfirmUpgrade with explicit stop: %v", err)
	}
	subs, _, err := mgr.DumpSubscriptions("a")
	if err != nil {
		t.Fatalf("DumpSubscriptions: %v", err)
	}
	if len(subs) != 1 || subs[0].ID != "s2" {
		t.Fatalf("only explicitly stopped s1 should be removed: %+v", subs)
	}
	out := streamCall(t, mgr, "stream_a", map[string]any{"action": "confirmed", "sub_id": "s2", "event": "e2", "pos": 2})
	if out["ok"] != "duplicate" {
		t.Fatalf("kept confirmed subscription must continue with dedupe: %+v", out)
	}
}
