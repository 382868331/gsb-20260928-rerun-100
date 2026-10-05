package host

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func keyedManifest(name string, caps ...string) Manifest {
	return Manifest{
		Name:     name,
		Path:     os.Args[0],
		Args:     []string{"-test.run=^TestMain$"},
		Env:      []string{"EXTSYS_TEST_HELPER=1", "EXTSYS_TEST_NAME=" + name, "EXTSYS_TEST_CAPS=" + strings.Join(caps, ",")},
		Provides: caps,
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func sleepEvent(id, key string, millis int) KeyedEvent {
	return KeyedEvent{ID: id, Key: key, Payload: map[string]int{"millis": millis}}
}

func confirmedOrder(j *Journal, key string) []string {
	var out []string
	for _, r := range j.KeyedRecords() {
		if r.Kind == KeyedConfirmed && r.Key == key {
			out = append(out, r.EventID)
		}
	}
	return out
}

func mustSubmit(t *testing.T, k *KeyedExecutor, ev KeyedEvent) SubmitOutcome {
	t.Helper()
	out, err := k.Submit(ev)
	if err != nil {
		t.Fatalf("submit %s: %v", ev.ID, err)
	}
	if !out.Accepted {
		t.Fatalf("submit %s: not accepted (duplicate=%v)", ev.ID, out.Duplicate)
	}
	return out
}

// TestKeyedOrderParallelBackpressure proves per-key ordering, real
// cross-key parallelism, duplicate suppression, and explainable
// backpressure when one slow key reaches its bounded queue limit.
func TestKeyedOrderParallelBackpressure(t *testing.T) {
	j := NewMemoryJournal()
	k, err := NewKeyedExecutor(KeyedConfig{
		Name:             "orders",
		Manifest:         keyedManifest("worker", "sleep", "echo"),
		Capability:       "sleep",
		Instances:        2,
		PerKeyQueueLimit: 3,
		Journal:          j,
	})
	if err != nil {
		t.Fatalf("NewKeyedExecutor: %v", err)
	}
	defer k.Close()

	// Hot key: h1 occupies an instance; h2..h4 fill the bounded queue.
	mustSubmit(t, k, sleepEvent("h1", "hot", 300))
	mustSubmit(t, k, sleepEvent("h2", "hot", 20))
	mustSubmit(t, k, sleepEvent("h3", "hot", 20))
	mustSubmit(t, k, sleepEvent("h4", "hot", 20))
	if got := k.Queued("hot"); got > 3 {
		t.Fatalf("in-memory queue for hot key exceeded limit: %d", got)
	}

	// The next event for the slow key is rejected with an explanation.
	_, err = k.Submit(sleepEvent("h5", "hot", 20))
	var bp *BackpressureError
	if !errors.As(err, &bp) {
		t.Fatalf("expected BackpressureError, got %v", err)
	}
	if bp.Key != "hot" || bp.Limit != 3 || bp.Queued != 3 {
		t.Fatalf("unexpected backpressure detail: %+v", bp)
	}
	if !strings.Contains(bp.Error(), "hot") || !strings.Contains(bp.Error(), "in flight") {
		t.Fatalf("backpressure not explainable: %s", bp.Error())
	}

	// Other keys are accepted and processed while the hot key is full.
	for i := 0; i < 4; i++ {
		mustSubmit(t, k, sleepEvent(fmt.Sprintf("c%d", i), fmt.Sprintf("cold-%d", i), 30))
	}

	waitFor(t, "all queues to drain", 10*time.Second, func() bool {
		st := k.Stats()
		return st.InFlight == 0 && st.QueuedTotal == 0
	})

	// Per-key order: hot key confirmations follow ingress order.
	got := confirmedOrder(j, "hot")
	want := []string{"h1", "h2", "h3", "h4"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("hot key confirm order = %v, want %v", got, want)
	}

	// Real parallelism: two 400ms events on different keys finish in
	// much less than their serial 800ms.
	start := time.Now()
	mustSubmit(t, k, sleepEvent("p1", "pa", 400))
	mustSubmit(t, k, sleepEvent("p2", "pb", 400))
	waitFor(t, "parallel events confirmed", 5*time.Second, func() bool {
		return k.Confirmed("p1") && k.Confirmed("p2")
	})
	if elapsed := time.Since(start); elapsed > 700*time.Millisecond {
		t.Fatalf("different keys did not run in parallel: %s", elapsed)
	}
	if k.Stats().MaxConcurrent < 2 {
		t.Fatalf("executor never ran two instances concurrently")
	}

	// A confirmed event is never re-executed.
	out, err := k.Submit(sleepEvent("h1", "hot", 20))
	if err != nil || !out.Duplicate {
		t.Fatalf("resubmitted confirmed event: out=%+v err=%v", out, err)
	}
	for _, r := range j.KeyedRecords() {
		if r.Kind == KeyedDispatched && r.EventID == "h1" && r.Seq == 0 {
			t.Fatalf("confirmed event h1 was redispatched")
		}
	}
	if n := len(confirmedOrder(j, "hot")); n != 4 {
		t.Fatalf("hot key confirmed %d times, want 4", n)
	}
}

// TestKeyedRescaleCrashRestart interleaves rescaling (2 -> 5 -> 1), a
// never-returning event, a late reply from a retired generation, an
// instance crash, and a full host restart, proving that confirmed
// work is never redone and unconfirmed work is never lost silently.
func TestKeyedRescaleCrashRestart(t *testing.T) {
	dir := t.TempDir()
	rec, err := OpenRecoveryLog(filepath.Join(dir, "recovery.jsonl"))
	if err != nil {
		t.Fatalf("OpenRecoveryLog: %v", err)
	}
	j := NewMemoryJournal()
	cfg := KeyedConfig{
		Name:             "stream",
		Manifest:         keyedManifest("worker", "sleep", "panic"),
		Capability:       "sleep",
		Instances:        2,
		PerKeyQueueLimit: 8,
		EventTimeout:     800 * time.Millisecond,
		ShutdownGrace:    2 * time.Second,
		Journal:          j,
		Recovery:         rec,
	}
	k, err := NewKeyedExecutor(cfg)
	if err != nil {
		t.Fatalf("NewKeyedExecutor: %v", err)
	}

	// One key never returns in time; hot and cold keys flow around it.
	mustSubmit(t, k, sleepEvent("s1", "stuck", 30000))
	for i := 1; i <= 3; i++ {
		mustSubmit(t, k, sleepEvent(fmt.Sprintf("h%d", i), "hot", 80))
	}
	for i := 1; i <= 6; i++ {
		mustSubmit(t, k, sleepEvent(fmt.Sprintf("c%d", i), fmt.Sprintf("cold-%d", i), 40))
	}

	// Scale 2 -> 5 -> 1 while events keep arriving; the subscription
	// is never stopped and nothing is replayed.
	if err := k.Rescale(5); err != nil {
		t.Fatalf("rescale to 5: %v", err)
	}
	for i := 7; i <= 10; i++ {
		mustSubmit(t, k, sleepEvent(fmt.Sprintf("c%d", i), fmt.Sprintf("cold-%d", i), 40))
	}
	if err := k.Rescale(1); err != nil {
		t.Fatalf("rescale to 1: %v", err)
	}
	if got := k.Stats().Desired; got != 1 {
		t.Fatalf("desired instances = %d, want 1", got)
	}

	// The stuck event times out: its key blocks with unknown effect,
	// its instance is retired, and the pool heals back to 1.
	waitFor(t, "stuck key blocked", 5*time.Second, func() bool {
		st := k.Stats()
		return len(st.BlockedKeys) == 1 && st.BlockedKeys[0] == "stuck"
	})
	waitFor(t, "pool healed to one instance", 5*time.Second, func() bool {
		return k.Stats().Instances == 1
	})
	waitFor(t, "hot and cold keys drained", 10*time.Second, func() bool {
		st := k.Stats()
		return st.InFlight == 0 && st.QueuedTotal == 0
	})
	if got := confirmedOrder(j, "hot"); strings.Join(got, ",") != "h1,h2,h3" {
		t.Fatalf("hot key confirm order = %v", got)
	}
	if k.Stats().MaxConcurrent < 2 {
		t.Fatalf("no cross-key parallelism observed during rescale")
	}
	if k.Confirmed("s1") {
		t.Fatalf("never-returning event must not be confirmed")
	}

	// A slow event on another key times out; its retired instance's
	// late reply is kept as evidence but confirms nothing.
	mustSubmit(t, k, sleepEvent("s2", "late", 1200))
	waitFor(t, "late key blocked", 5*time.Second, func() bool {
		for _, key := range k.Stats().BlockedKeys {
			if key == "late" {
				return true
			}
		}
		return false
	})
	waitFor(t, "late reply recorded as evidence", 5*time.Second, func() bool {
		for _, r := range j.KeyedRecords() {
			if r.Kind == KeyedLateReply && r.EventID == "s2" {
				return true
			}
		}
		return false
	})
	if k.Confirmed("s2") {
		t.Fatalf("late reply from a retired generation must not confirm its event")
	}
	// Manual resolution unblocks the key; a new event on it is
	// confirmed only by its own dispatch, not by the old reply.
	if err := k.ResolveKey("late", ResolveDrop); err != nil {
		t.Fatalf("resolve late: %v", err)
	}
	mustSubmit(t, k, sleepEvent("s3", "late", 20))
	waitFor(t, "s3 confirmed", 5*time.Second, func() bool { return k.Confirmed("s3") })

	// An instance dies mid-event: only that key blocks; other keys
	// keep flowing on the surviving instance.
	mustSubmit(t, k, KeyedEvent{ID: "b1", Key: "boom", Cap: "panic", Payload: map[string]int{}})
	waitFor(t, "boom key blocked", 5*time.Second, func() bool {
		for _, key := range k.Stats().BlockedKeys {
			if key == "boom" {
				return true
			}
		}
		return false
	})
	mustSubmit(t, k, sleepEvent("c11", "cold-1", 20))
	waitFor(t, "c11 confirmed after crash", 5*time.Second, func() bool { return k.Confirmed("c11") })
	if err := k.ResolveKey("boom", ResolveDrop); err != nil {
		t.Fatalf("resolve boom: %v", err)
	}

	// Duplicates of confirmed events are rejected even before restart.
	if out, _ := k.Submit(sleepEvent("h1", "hot", 10)); !out.Duplicate {
		t.Fatalf("confirmed h1 not deduplicated: %+v", out)
	}
	last := mustSubmit(t, k, sleepEvent("c12", "cold-2", 10))
	waitFor(t, "c12 confirmed", 5*time.Second, func() bool { return k.Confirmed("c12") })

	// Cancel the subscription: the never-resolved stuck event is
	// reported, not dropped silently.
	report := k.Close()
	found := false
	for _, id := range report.Unconfirmed {
		if id == "s1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("close report lost unconfirmed stuck event: %v", report.Unconfirmed)
	}
	_ = rec.Close()

	// Host restart: a new executor over the same state directory must
	// not redo any confirmed event, and the ingress sequence continues.
	rec2, err := OpenRecoveryLog(filepath.Join(dir, "recovery.jsonl"))
	if err != nil {
		t.Fatalf("reopen recovery: %v", err)
	}
	j2 := NewMemoryJournal()
	cfg.Journal = j2
	cfg.Recovery = rec2
	cfg.Instances = 2
	k2, err := NewKeyedExecutor(cfg)
	if err != nil {
		t.Fatalf("NewKeyedExecutor after restart: %v", err)
	}
	defer k2.Close()
	for _, id := range []string{"h1", "h2", "h3", "c1", "c11", "c12", "s3"} {
		out, err := k2.Submit(sleepEvent(id, "whatever", 5))
		if err != nil || !out.Duplicate {
			t.Fatalf("confirmed event %s reaccepted after restart: %+v %v", id, out, err)
		}
	}
	for _, r := range j2.KeyedRecords() {
		if r.Kind == KeyedDispatched {
			t.Fatalf("confirmed event redispatched after restart: %+v", r)
		}
	}
	out := mustSubmit(t, k2, sleepEvent("n1", "fresh", 10))
	if out.Seq <= last.Seq {
		t.Fatalf("ingress sequence restarted: old %d, new %d", last.Seq, out.Seq)
	}
	waitFor(t, "n1 confirmed", 5*time.Second, func() bool { return k2.Confirmed("n1") })
}

// TestKeyedUnsubscribeReleases shows that cancelling a subscription
// drains the in-flight event, releases the instances, and reports the
// queued events of every other key instead of dropping them.
func TestKeyedUnsubscribeReleases(t *testing.T) {
	k, err := NewKeyedExecutor(KeyedConfig{
		Name:          "sub",
		Manifest:      keyedManifest("worker", "sleep"),
		Capability:    "sleep",
		Instances:     1,
		ShutdownGrace: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewKeyedExecutor: %v", err)
	}
	mustSubmit(t, k, sleepEvent("e1", "a", 200))
	mustSubmit(t, k, sleepEvent("e2", "a", 10))
	mustSubmit(t, k, sleepEvent("e3", "b", 10))
	report := k.Close()
	if !k.Confirmed("e1") {
		t.Fatalf("in-flight event must confirm during drain")
	}
	un := strings.Join(report.Unconfirmed, ",")
	if !strings.Contains(un, "e2") || !strings.Contains(un, "e3") {
		t.Fatalf("queued events of other keys dropped on unsubscribe: %v", report.Unconfirmed)
	}
	for _, id := range report.Unconfirmed {
		if id == "e1" {
			t.Fatalf("confirmed event reported unconfirmed: %v", report.Unconfirmed)
		}
	}
	if _, err := k.Submit(sleepEvent("e4", "a", 1)); err == nil {
		t.Fatalf("submit after unsubscribe must fail")
	}
}
