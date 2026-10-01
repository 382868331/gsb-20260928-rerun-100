package host

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"extsys/internal/extsdk"
	"extsys/internal/protocol"
)

// helperExt is a synthetic extension used via the test-binary-as-child
// process trick (see TestMain). It registers only the capabilities named
// in EXTSYS_TEST_CAPS so manifest enforcement can be exercised.
func helperExt() {
	name := os.Getenv("EXTSYS_TEST_NAME")
	version := os.Getenv("EXTSYS_TEST_VERSION")
	if version == "" {
		version = "3.1.4-test"
	}
	caps := map[string]bool{}
	for _, c := range strings.Split(os.Getenv("EXTSYS_TEST_CAPS"), ",") {
		if c != "" {
			caps[c] = true
		}
	}
	ext := extsdk.New(name, version)
	if d := os.Getenv("EXTSYS_TEST_SHUTDOWN_DELAY"); d != "" {
		millis, _ := strconv.Atoi(d)
		ext.OnShutdown = func() { time.Sleep(time.Duration(millis) * time.Millisecond) }
	}
	if os.Getenv("EXTSYS_TEST_REJECT_HANDSHAKE") == "1" {
		// Read the host hello, then refuse the handshake and exit.
		var hello protocol.Message
		if err := json.NewDecoder(os.Stdin).Decode(&hello); err == nil {
			_ = protocol.Encode(os.Stdout, &protocol.Message{
				Type: protocol.TypeError,
				Err:  "handshake rejected by test build",
			})
		}
		os.Exit(1)
	}
	if caps["echo"] {
		ext.Handle("echo", 1, func(p json.RawMessage) (any, error) {
			return map[string]json.RawMessage{"echoed": p}, nil
		})
	}
	if caps["sha256"] {
		ext.Handle("sha256", 1, func(p json.RawMessage) (any, error) {
			return map[string]string{"algo": "sha256", "of": string(p)}, nil
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
		Env:      helperEnv(name, strings.Join(all, ","), nil),
		Provides: provides,
		Requires: requires,
	}
}

// helperEnv builds the child-process environment. extra is appended as
// fully formed KEY=value entries (version override, slow shutdown, ...).
func helperEnv(name, caps string, extra []string) []string {
	env := []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"EXTSYS_TEST_HELPER=1",
		"EXTSYS_TEST_NAME=" + name,
		"EXTSYS_TEST_CAPS=" + caps,
	}
	return append(env, extra...)
}

// versionedManifest returns a manifest for a build advertising ver.
func versionedManifest(name, ver string, provides, requires []string, extra ...string) Manifest {
	env := helperEnv(name, strings.Join(provides, ","), append(extra, "EXTSYS_TEST_VERSION="+ver))
	return Manifest{
		Name:     name,
		Path:     os.Args[0],
		Args:     []string{"-test.run=^TestMain$"},
		Env:      env,
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

// eventsOf returns the recorded switch events for one upgrade id.
func eventsOf(t *testing.T, j *Journal, upgradeID string) []string {
	t.Helper()
	var out []string
	for _, r := range j.SwitchRecords() {
		if r.UpgradeID == upgradeID {
			out = append(out, r.Event)
		}
	}
	return out
}

func hasEvent(events []string, want string) bool {
	for _, e := range events {
		if e == want {
			return true
		}
	}
	return false
}

// TestUpgradeServesOldDuringPrepareAndCutsOver: while the new build is
// starting, requests are still answered by the old session; only after
// every candidate is confirmed does traffic move to the new version.
func TestUpgradeServesOldDuringPrepareAndCutsOver(t *testing.T) {
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: j}
	mgr := testManager(t, []Manifest{
		versionedManifest("hash", "1.0.0", []string{"sha256"}, nil),
		versionedManifest("text", "1.0.0", []string{"echo"}, []string{"sha256"}),
	}, cfg)

	oldHashSession := mgr.Session("hash")
	oldTextSession := mgr.Session("text")
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("pre-upgrade call: %v", err)
	}

	candidates := []Manifest{
		versionedManifest("hash", "2.0.0", []string{"sha256"}, nil),
		versionedManifest("text", "2.0.0", []string{"echo"}, []string{"sha256"}),
	}
	if err := mgr.Upgrade(candidates); err != nil {
		t.Fatalf("Upgrade: %v", err)
	}
	if got := mgr.Version("hash"); got != "2.0.0" {
		t.Fatalf("hash version = %q, want 2.0.0", got)
	}
	if got := mgr.Version("text"); got != "2.0.0" {
		t.Fatalf("text version = %q, want 2.0.0", got)
	}
	newHashSession := mgr.Session("hash")
	newTextSession := mgr.Session("text")
	if newHashSession == oldHashSession || newTextSession == oldTextSession {
		t.Fatal("session tokens must be fresh after upgrade")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("post-upgrade call: %v", err)
	}

	ev := eventsOf(t, j, "upgrade-1")
	for _, want := range []string{SwitchValidate, SwitchPrepared, SwitchCutover, SwitchDrained, SwitchMigrated, SwitchComplete} {
		if !hasEvent(ev, want) {
			t.Fatalf("upgrade audit trail missing %q: %v", want, ev)
		}
	}

	// Every task record must name the exact version/session that served it.
	for _, rec := range j.TaskRecords() {
		if rec.ExtVersion == "" || rec.Session == "" {
			t.Fatalf("task record lacks version/session: %+v", rec)
		}
	}
}

// TestUpgradeRejectedCandidateKeepsOldBuild: a dependency candidate
// refuses the handshake. No traffic is cut over: both old builds keep
// serving with their original sessions, and the failed candidate is
// distinguishable in the audit trail from the still-serving old one.
func TestUpgradeRejectedCandidateKeepsOldBuild(t *testing.T) {
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: j}
	mgr := testManager(t, []Manifest{
		versionedManifest("hash", "1.0.0", []string{"sha256"}, nil),
		versionedManifest("text", "1.0.0", []string{"echo"}, []string{"sha256"}),
	}, cfg)
	oldHashSession := mgr.Session("hash")
	oldTextSession := mgr.Session("text")

	candidates := []Manifest{
		versionedManifest("hash", "2.0.0", []string{"sha256"}, nil, "EXTSYS_TEST_REJECT_HANDSHAKE=1"),
		versionedManifest("text", "2.0.0", []string{"echo"}, []string{"sha256"}),
	}
	if err := mgr.Upgrade(candidates); err == nil {
		t.Fatal("expected upgrade to fail when a candidate rejects the handshake")
	}

	if got := mgr.Version("hash"); got != "1.0.0" {
		t.Fatalf("hash must stay on old version, got %q", got)
	}
	if got := mgr.Version("text"); got != "1.0.0" {
		t.Fatalf("text must stay on old version, got %q", got)
	}
	if mgr.Session("hash") != oldHashSession || mgr.Session("text") != oldTextSession {
		t.Fatal("old sessions must remain the owners after a failed upgrade")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("old combination must still serve after failed upgrade: %v", err)
	}

	ev := eventsOf(t, j, "upgrade-1")
	if !hasEvent(ev, SwitchAbort) && !hasEvent(ev, SwitchRollback) {
		t.Fatalf("expected abort/rollback records, got %v", ev)
	}
	if hasEvent(ev, SwitchCutover) || hasEvent(ev, SwitchComplete) {
		t.Fatalf("failed upgrade must not cut over: %v", ev)
	}
}

// TestFailedUpgradeRetryUsesFreshSessions: a failed attempt must not
// leak session identities into the retry; the successful retry's
// sessions are distinct from both the old ones and the cancelled ones.
func TestFailedUpgradeRetryUsesFreshSessions(t *testing.T) {
	mk := func(ver, reject string) Manifest {
		var extra []string
		if reject != "" {
			extra = append(extra, "EXTSYS_TEST_REJECT_HANDSHAKE=1")
		}
		return versionedManifest("a", ver, []string{"echo"}, nil, extra...)
	}
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: j}
	mgr := testManager(t, []Manifest{mk("1.0.0", "")}, cfg)
	s0 := mgr.Session("a")

	if err := mgr.Upgrade([]Manifest{mk("2.0.0", "x")}); err == nil {
		t.Fatal("expected first attempt to fail")
	}
	if mgr.Session("a") != s0 || mgr.Version("a") != "1.0.0" {
		t.Fatal("old build must remain after failed attempt")
	}
	if err := mgr.Upgrade([]Manifest{mk("2.0.0", "")}); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if mgr.Version("a") != "2.0.0" || mgr.Session("a") == s0 {
		t.Fatal("retry must run a fresh 2.0.0 process with a new session")
	}

	s1 := mgr.Session("a")
	if err := mgr.Upgrade([]Manifest{mk("3.0.0", "")}); err != nil {
		t.Fatalf("second upgrade: %v", err)
	}
	if mgr.Version("a") != "3.0.0" {
		t.Fatalf("expected 3.0.0")
	}
	if s := mgr.Session("a"); s == s0 || s == s1 {
		t.Fatalf("consecutive upgrades must rotate session tokens: %s", s)
	}
}

// TestUpgradeDrainOverrunRecordsRealReason: the old process takes
// longer than the agreed drain boundary. Cutover still happens, but the
// upgrade is not declared fully complete and the kill is recorded.
func TestUpgradeDrainOverrunRecordsRealReason(t *testing.T) {
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: 500 * time.Millisecond, Journal: j}
	mgr := testManager(t,
		[]Manifest{versionedManifest("a", "1.0.0", []string{"echo"}, nil, "EXTSYS_TEST_SHUTDOWN_DELAY=5000")},
		cfg)

	err := mgr.Upgrade([]Manifest{versionedManifest("a", "2.0.0", []string{"echo"}, nil)})
	if err == nil {
		t.Fatal("expected error when the old session overruns the drain boundary")
	}
	if !strings.Contains(err.Error(), "not clean") {
		t.Fatalf("error should explain the handoff was not clean: %v", err)
	}
	if got := mgr.Version("a"); got != "2.0.0" {
		t.Fatalf("traffic must already be on the new build, got %q", got)
	}
	ev := eventsOf(t, j, "upgrade-1")
	if !hasEvent(ev, SwitchDrainKilled) || !hasEvent(ev, SwitchIncomplete) {
		t.Fatalf("expected drain_kill + incomplete records, got %v", ev)
	}
	if hasEvent(ev, SwitchComplete) || hasEvent(ev, SwitchMigrated) {
		t.Fatalf("overrunning handoff must not be reported as migrated/complete: %v", ev)
	}
}

// TestUpgradeOldExitDuringPrepareKeepsServingOthers: while candidates
// are preparing, the old hash process exits on its own. The upgrade is
// aborted (a healthy dependency cannot be confirmed), candidates are
// torn down, and the audit trail clearly separates the cancelled
// candidate from the still-serving old text process.
func TestUpgradeOldExitDuringPrepareKeepsServingOthers(t *testing.T) {
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: j}
	mgr := testManager(t, []Manifest{
		versionedManifest("hash", "1.0.0", []string{"sha256"}, nil),
		versionedManifest("text", "1.0.0", []string{"echo"}, []string{"sha256"}),
	}, cfg)

	// Kill the old hash process directly to simulate an independent
	// exit during upgrade preparation.
	mgr.mu.Lock()
	oldHash := mgr.instances["hash"]
	oldTextSession := mgr.instances["text"].session
	mgr.mu.Unlock()
	oldHash.killNow()

	candidates := []Manifest{
		versionedManifest("hash", "2.0.0", []string{"sha256"}, nil, "EXTSYS_TEST_REJECT_HANDSHAKE=1"),
		versionedManifest("text", "2.0.0", []string{"echo"}, []string{"sha256"}),
	}
	if err := mgr.Upgrade(candidates); err == nil {
		t.Fatal("expected upgrade failure")
	}

	// The surviving old text session is untouched.
	if mgr.Alive("text") != true {
		t.Fatal("unrelated old text process must keep serving")
	}
	if mgr.Session("text") != oldTextSession || mgr.Version("text") != "1.0.0" {
		t.Fatal("surviving old session must retain its identity")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("old text process still serves: %v", err)
	}

	// Abort records mention old version retained; there is no cutover.
	var sawAbort bool
	for _, rec := range j.SwitchRecords() {
		if rec.UpgradeID != "upgrade-1" {
			continue
		}
		if rec.Event == SwitchAbort || rec.Event == SwitchRollback {
			sawAbort = true
			if rec.NewVersion != "" {
				t.Fatalf("aborted candidate must not be recorded as live: %+v", rec)
			}
		}
		if rec.Event == SwitchCutover {
			t.Fatal("no cutover allowed when a candidate failed")
		}
	}
	if !sawAbort {
		t.Fatal("expected abort records distinguishing cancelled candidates")
	}
}

// TestJournalAttributesEveryRequestToItsServingSession: requests made
// before and after an upgrade are journaled against the exact version
// and session token that handled them, with no shared identity.
func TestJournalAttributesEveryRequestToItsServingSession(t *testing.T) {
	j := NewMemoryJournal()
	cfg := Config{TaskTimeout: time.Second, ShutdownGrace: time.Second, Journal: j}
	mgr := testManager(t, []Manifest{
		versionedManifest("a", "1.0.0", []string{"echo"}, nil),
	}, cfg)
	s0 := mgr.Session("a")
	mgr.Call("echo", nil)
	mgr.Call("echo", nil)
	if err := mgr.Upgrade([]Manifest{versionedManifest("a", "2.0.0", []string{"echo"}, nil)}); err != nil {
		t.Fatalf("upgrade: %v", err)
	}
	s1 := mgr.Session("a")
	mgr.Call("echo", nil)
	if err := mgr.Upgrade([]Manifest{versionedManifest("a", "3.0.0", []string{"echo"}, nil)}); err != nil {
		t.Fatalf("upgrade2: %v", err)
	}
	s2 := mgr.Session("a")
	mgr.Call("echo", nil)

	recs := j.TaskRecords()
	if len(recs) != 4 {
		t.Fatalf("want 4 task records, got %d", len(recs))
	}
	want := []struct{ ver, sess string }{
		{"1.0.0", s0}, {"1.0.0", s0}, {"2.0.0", s1}, {"3.0.0", s2},
	}
	for i, w := range want {
		if recs[i].ExtVersion != w.ver || recs[i].Session != w.sess {
			t.Fatalf("record %d = %s/%s, want %s/%s", i, recs[i].ExtVersion, recs[i].Session, w.ver, w.sess)
		}
	}
	if s0 == s1 || s1 == s2 || s0 == s2 {
		t.Fatal("sessions across consecutive upgrades must be distinct")
	}
}
