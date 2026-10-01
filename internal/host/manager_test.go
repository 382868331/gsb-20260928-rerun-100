package host

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"extsys/internal/extsdk"
)

// helperExt is a synthetic extension used via the test-binary-as-child
// process trick (see TestMain). It registers only the capabilities named
// in EXTSYS_TEST_CAPS so manifest enforcement can be exercised.
func helperExt() {
	name := os.Getenv("EXTSYS_TEST_NAME")
	if code := os.Getenv("EXTSYS_TEST_EXIT"); code != "" {
		// Simulates a candidate that dies before finishing (or
		// before even starting) the handshake.
		os.Exit(3)
	}
	caps := map[string]bool{}
	for _, c := range strings.Split(os.Getenv("EXTSYS_TEST_CAPS"), ",") {
		if c != "" {
			caps[c] = true
		}
	}
	version := os.Getenv("EXTSYS_TEST_VER")
	if version == "" {
		version = "3.1.4-test"
	}
	ext := extsdk.New(name, version)
	if caps["echo"] {
		ext.Handle("echo", 1, func(p json.RawMessage) (any, error) {
			return map[string]json.RawMessage{"echoed": p}, nil
		})
	}
	if caps["sleep"] {
		ext.Handle("sleep", 1, func(p json.RawMessage) (any, error) {
			var in struct {
				Millis int `json:"millis"`
			}
			if err := json.Unmarshal(p, &in); err != nil {
				return nil, err
			}
			time.Sleep(time.Duration(in.Millis) * time.Millisecond)
			return map[string]string{"ok": "slept"}, nil
		})
	}
	if caps["panic"] {
		ext.Handle("panic", 1, func(p json.RawMessage) (any, error) {
			os.Exit(2)
			return nil, nil
		})
	}
	if err := ext.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func TestMain(m *testing.M) {
	if os.Getenv("EXTSYS_TEST_HELPER") == "1" {
		helperExt()
		return
	}
	os.Exit(m.Run())
}

func helperManifest(name string, provides []string, requires []string) Manifest {
	all := append(append([]string{}, provides...), requires...)
	return Manifest{
		Name:     name,
		Path:     os.Args[0],
		Args:     []string{"-test.run=^TestMain$"},
		Env:      []string{"EXTSYS_TEST_HELPER=1", "EXTSYS_TEST_NAME=" + name, "EXTSYS_TEST_CAPS=" + strings.Join(all, ",")},
		Provides: provides,
		Requires: requires,
	}
}

func testManager(t *testing.T, manifests []Manifest, cfg Config) *Manager {
	t.Helper()
	cfg.HandshakeTimeout = 2 * time.Second
	mgr, err := New(manifests, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(mgr.Shutdown)
	return mgr
}

func TestRequiresMissingCapabilityFailsBeforeStart(t *testing.T) {
	mf := []Manifest{
		helperManifest("a", []string{"echo"}, []string{"nonexistent"}),
	}
	if _, err := New(mf, Config{}); err == nil {
		t.Fatal("expected startup-time dependency error")
	}
}

func TestDependencyOrder(t *testing.T) {
	mf := []Manifest{
		helperManifest("a", []string{"echo"}, nil),
		helperManifest("b", []string{"sleep"}, []string{"echo"}),
	}
	mgr, err := New(mf, Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := mgr.order[0]; got != "a" {
		t.Fatalf("provider a must start first, got order %v", mgr.order)
	}
}

func TestDependencyCycleRejected(t *testing.T) {
	mf := []Manifest{
		helperManifest("a", []string{"echo"}, []string{"sleep"}),
		helperManifest("b", []string{"sleep"}, []string{"echo"}),
	}
	if _, err := New(mf, Config{}); err == nil {
		t.Fatal("expected cycle error")
	}
}

func TestUndeclaredCapabilityRejected(t *testing.T) {
	mf := []Manifest{{
		Name:     "a",
		Path:     os.Args[0],
		Args:     []string{"-test.run=^TestMain$"},
		Env:      []string{"EXTSYS_TEST_HELPER=1", "EXTSYS_TEST_NAME=a", "EXTSYS_TEST_CAPS=echo,sleep"},
		Provides: []string{"echo"},
	}}
	mgr, err := New(mf, Config{HandshakeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := mgr.Start(); err == nil {
		mgr.Shutdown()
		t.Fatal("expected incompatibility error for undeclared capability")
	}
}

func TestCallAndTimeoutAndCrashIsolation(t *testing.T) {
	cfg := Config{TaskTimeout: 200 * time.Millisecond, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{
		helperManifest("a", []string{"echo"}, nil),
		helperManifest("b", []string{"sleep", "panic"}, nil),
	}, cfg)

	raw, err := mgr.Call("echo", map[string]string{"hi": "there"})
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	var got struct {
		Echoed struct {
			Hi string `json:"hi"`
		} `json:"echoed"`
	}
	if err := json.Unmarshal(raw, &got); err != nil || got.Echoed.Hi != "there" {
		t.Fatalf("bad echo result: %s err=%v", raw, err)
	}

	// A slow task in b times out without affecting a.
	if _, err := mgr.Call("sleep", map[string]int{"millis": 2000}); err == nil {
		t.Fatal("expected timeout error")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("extension a should be unaffected by b's slow task: %v", err)
	}

	// A crash in b only fails b's requests. The panic task may be
	// queued behind the still-sleeping slow task, so wait for the
	// process death rather than assuming immediate delivery.
	if _, err := mgr.Call("panic", nil); err == nil {
		t.Fatal("expected crash error")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && mgr.Alive("b") {
		time.Sleep(20 * time.Millisecond)
	}
	if mgr.Alive("b") {
		t.Fatal("b should be marked dead after panic")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("extension a should survive b's crash: %v", err)
	}
}

func TestRestartRotatesSessionToken(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{
		helperManifest("a", []string{"echo"}, nil),
	}, cfg)

	old := mgr.Session("a")
	if old == "" {
		t.Fatal("no session after start")
	}
	if err := mgr.Restart("a"); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	newTok := mgr.Session("a")
	if newTok == "" || newTok == old {
		t.Fatalf("session token must change on restart: old=%q new=%q", old, newTok)
	}
	if _, err := mgr.Call("echo", map[string]int{"x": 1}); err != nil {
		t.Fatalf("call after restart: %v", err)
	}
}

func TestCallsRejectedAfterShutdownBegins(t *testing.T) {
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second}
	mgr := testManager(t, []Manifest{
		helperManifest("a", []string{"echo"}, nil),
	}, cfg)
	mgr.Shutdown()
	if _, err := mgr.Call("echo", nil); err == nil {
		t.Fatal("expected calls to be rejected after shutdown")
	}
}

// withEnv returns a copy of the manifest with extra KEY=VALUE env
// entries appended, used to steer the helper binary per candidate.
func withEnv(mf Manifest, kv ...string) Manifest {
	mf.Env = append(append([]string{}, mf.Env...), kv...)
	return mf
}

func switchEvents(j *Journal) []string {
	var out []string
	for _, r := range j.SwitchRecords() {
		out = append(out, r.Event)
	}
	return out
}

func countEvent(j *Journal, event string) int {
	n := 0
	for _, r := range j.SwitchRecords() {
		if r.Event == event {
			n++
		}
	}
	return n
}

func TestUpgradeCutoverAuditAndSessionIdentity(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	bManifest := helperManifest("b", []string{"sleep"}, nil)
	bManifest.Requires = []string{"echo"} // depends on a's echo capability
	mgr := testManager(t, []Manifest{
		helperManifest("a", []string{"echo"}, nil),
		bManifest,
	}, cfg)

	if _, err := mgr.Call("echo", map[string]int{"x": 1}); err != nil {
		t.Fatalf("pre-upgrade call: %v", err)
	}
	oldA, oldB := mgr.Session("a"), mgr.Session("b")

	candidates := []Manifest{
		withEnv(helperManifest("a", []string{"echo"}, nil), "EXTSYS_TEST_VER=2.0.0"),
		withEnv(bManifest, "EXTSYS_TEST_VER=2.0.0"),
	}
	if err := mgr.Upgrade(candidates); err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	newA, newB := mgr.Session("a"), mgr.Session("b")
	if newA == oldA || newB == oldB || newA == "" || newB == "" {
		t.Fatalf("upgrade must rotate session tokens: old=(%q,%q) new=(%q,%q)", oldA, oldB, newA, newB)
	}
	if _, err := mgr.Call("echo", map[string]int{"x": 2}); err != nil {
		t.Fatalf("post-upgrade call: %v", err)
	}

	// Every request is attributable to exactly one version/session.
	tasks := journal.TaskRecords()
	if len(tasks) != 2 {
		t.Fatalf("expected 2 task records, got %d", len(tasks))
	}
	if tasks[0].ExtVersion != "3.1.4-test" || tasks[0].Session != oldA {
		t.Fatalf("first call must be served by old build: %+v", tasks[0])
	}
	if tasks[1].ExtVersion != "2.0.0" || tasks[1].Session != newA {
		t.Fatalf("second call must be served by new build: %+v", tasks[1])
	}

	// Full audit trail: validate -> prepared -> cutover -> drained ->
	// activated, and nothing is announced activated before its drain
	// ended with a real reason.
	events := switchEvents(journal)
	if countEvent(journal, SwitchValidate) != 1 || countEvent(journal, SwitchPrepared) != 2 ||
		countEvent(journal, SwitchCutover) != 2 || countEvent(journal, SwitchDrained) != 2 ||
		countEvent(journal, SwitchActivated) != 2 {
		t.Fatalf("incomplete audit trail: %v", events)
	}
	firstActivated := -1
	lastDrain := -1
	for i, e := range events {
		switch e {
		case SwitchActivated:
			if firstActivated == -1 {
				firstActivated = i
			}
		case SwitchDrained, SwitchDrainKilled, SwitchHostClose:
			lastDrain = i
		}
	}
	if firstActivated < lastDrain {
		t.Fatalf("activated announced before all drains finished: %v", events)
	}
}

func TestUpgradeFailureRetainsOldThenRetryGetsFreshSession(t *testing.T) {
	journal := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: journal}
	mgr := testManager(t, []Manifest{
		helperManifest("a", []string{"echo"}, nil),
	}, cfg)

	oldSession := mgr.Session("a")
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("pre-upgrade call: %v", err)
	}

	// Candidate process exits before the handshake: the upgrade must
	// fail and the old combination must keep serving untouched.
	dead := withEnv(helperManifest("a", []string{"echo"}, nil), "EXTSYS_TEST_EXIT=1")
	if err := mgr.Upgrade([]Manifest{dead}); err == nil {
		t.Fatal("expected upgrade failure for dying candidate")
	}
	if got := mgr.Session("a"); got != oldSession {
		t.Fatalf("failed upgrade must not touch the running session: was %q now %q", oldSession, got)
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("old version must keep serving after failed upgrade: %v", err)
	}
	if countEvent(journal, SwitchCutover) != 0 || countEvent(journal, SwitchActivated) != 0 {
		t.Fatalf("failed upgrade must not cut over: %v", switchEvents(journal))
	}
	if countEvent(journal, SwitchAbort) == 0 {
		t.Fatalf("cancelled candidate must be recorded as abort: %v", switchEvents(journal))
	}

	// Retrying after the failure must produce a fresh session identity,
	// never reuse the cancelled candidate's or the old one.
	good := withEnv(helperManifest("a", []string{"echo"}, nil), "EXTSYS_TEST_VER=2.0.0")
	if err := mgr.Upgrade([]Manifest{good}); err != nil {
		t.Fatalf("retry upgrade: %v", err)
	}
	retried := mgr.Session("a")
	if retried == "" || retried == oldSession {
		t.Fatalf("retry must use a fresh session: old=%q retried=%q", oldSession, retried)
	}

	// A second consecutive upgrade rotates identity again; no session
	// is ever shared between attempts.
	good3 := withEnv(helperManifest("a", []string{"echo"}, nil), "EXTSYS_TEST_VER=3.0.0")
	if err := mgr.Upgrade([]Manifest{good3}); err != nil {
		t.Fatalf("second upgrade: %v", err)
	}
	third := mgr.Session("a")
	if third == "" || third == retried || third == oldSession {
		t.Fatalf("consecutive upgrades must not mix session identities: %q %q %q", oldSession, retried, third)
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("call after consecutive upgrades: %v", err)
	}
	tasks := journal.TaskRecords()
	last := tasks[len(tasks)-1]
	if last.ExtVersion != "3.0.0" || last.Session != third {
		t.Fatalf("last call must be served by v3 session %q: %+v", third, last)
	}
}
