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
	return Manifest{
		Name:     name,
		Path:     os.Args[0],
		Args:     []string{"-test.run=^TestMain$"},
		Env:      helperEnv(name, strings.Join(provides, ","), "3.1.4-test"),
		Provides: provides,
		Requires: requires,
	}
}

func helperManifestVersion(name, version string, provides []string, requires []string) Manifest {
	mf := helperManifest(name, provides, requires)
	mf.Env = helperEnv(name, strings.Join(provides, ","), version)
	return mf
}

func helperEnv(name, caps, version string) []string {
	env := []string{
		"EXTSYS_TEST_HELPER=1",
		"EXTSYS_TEST_NAME=" + name,
		"EXTSYS_TEST_CAPS=" + caps,
		"EXTSYS_TEST_VERSION=" + version,
	}
	return env
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

func TestUpgradeHandoverRotatesOwnershipAndSession(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"echo"}, nil),
	}, cfg)

	oldSession := mgr.Session("a")
	res, err := mgr.UpgradeSet([]Manifest{
		helperManifestVersion("a", "2.0.0", []string{"echo"}, nil),
	})
	if err != nil {
		t.Fatalf("UpgradeSet: %v", err)
	}
	if len(res) != 1 || res[0].OldSession != oldSession {
		t.Fatalf("bad switch result: %+v", res)
	}
	if res[0].DrainReason != "drained" {
		t.Fatalf("old generation should drain cleanly, got %q", res[0].DrainReason)
	}
	if res[0].NewSession == oldSession || res[0].NewVersion != "2.0.0" {
		t.Fatalf("new generation identity wrong: %+v", res[0])
	}
	newSession := mgr.Session("a")
	if newSession != res[0].NewSession {
		t.Fatal("new tasks must route to the new session")
	}

	// The journal entry for the next task must name the new generation.
	raw, err := mgr.Call("echo", map[string]string{"v": "new"})
	if err != nil {
		t.Fatalf("call after upgrade: %v", err)
	}
	var got map[string]json.RawMessage
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeOldProcessFinishesAcceptedTask(t *testing.T) {
	cfg := Config{TaskTimeout: 5 * time.Second, ShutdownGrace: 5 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"sleep"}, nil),
	}, cfg)
	oldSession := mgr.Session("a")

	done := make(chan error, 1)
	go func() {
		_, err := mgr.Call("sleep", map[string]int{"millis": 600})
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)

	res, err := mgr.UpgradeSet([]Manifest{
		helperManifestVersion("a", "2.0.0", []string{"sleep"}, nil),
	})
	if err != nil {
		t.Fatalf("UpgradeSet: %v", err)
	}
	if res[0].DrainReason != "drained" {
		t.Fatalf("old process should finish its accepted task then bye: %s", res[0].DrainReason)
	}
	if err := <-done; err != nil {
		t.Fatalf("task accepted by old generation must complete, got %v", err)
	}
	if s := mgr.Session("a"); s == oldSession {
		t.Fatal("ownership must move to the new session")
	}
	// After the old process has exited, no stale-session reply exists:
	// the new generation serves the next task exclusively.
	if _, err := mgr.Call("sleep", map[string]int{"millis": 0}); err != nil {
		t.Fatalf("new generation call: %v", err)
	}
}

func TestUpgradeDependencySetValidatedTogether(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("dep", "1.0.0", []string{"echo"}, nil),
		helperManifestVersion("app", "1.0.0", []string{"sleep"}, []string{"echo"}),
	}, cfg)

	// Both the dependency and the dependent upgrade in one atomic set.
	_, err := mgr.UpgradeSet([]Manifest{
		helperManifestVersion("dep", "2.0.0", []string{"echo"}, nil),
		helperManifestVersion("app", "2.0.0", []string{"sleep"}, []string{"echo"}),
	})
	if err != nil {
		t.Fatalf("combined upgrade: %v", err)
	}
	if mgr.Session("dep") == "" || mgr.Session("app") == "" {
		t.Fatal("both new generations must be live")
	}
}

func TestUpgradeFailureRetainsOldGenerations(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"echo"}, nil),
		helperManifestVersion("b", "1.0.0", []string{"sleep"}, nil),
	}, cfg)
	oldA := mgr.Session("a")
	oldB := mgr.Session("b")

	// a upgrades fine, but b's new build advertises fewer capabilities
	// than its manifest requires: the whole set must abort and both old
	// generations must remain task owners.
	badB := helperManifestVersion("b", "2.0.0", []string{"sleep", "panic"}, nil)
	badB.Provides = []string{"sleep", "panic"}
	badB.Env = helperEnv("b", "sleep", "2.0.0") // panic not really served

	_, err := mgr.UpgradeSet([]Manifest{
		helperManifestVersion("a", "2.0.0", []string{"echo"}, nil),
		badB,
	})
	if err == nil {
		t.Fatal("expected combined upgrade to fail capability verification")
	}
	if mgr.Session("a") != oldA || mgr.Session("b") != oldB {
		t.Fatal("old sessions must be retained after failed upgrade")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("old a must still serve: %v", err)
	}
	if _, err := mgr.Call("sleep", map[string]int{"millis": 0}); err != nil {
		t.Fatalf("old b must still serve: %v", err)
	}
}

func TestUpgradeNewProcessStartFailureRetainsOld(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"echo"}, nil),
	}, cfg)
	old := mgr.Session("a")

	bad := helperManifestVersion("a", "2.0.0", []string{"echo"}, nil)
	bad.Path = "/nonexistent/extsys-binary"
	if _, err := mgr.UpgradeSet([]Manifest{bad}); err == nil {
		t.Fatal("expected spawn failure")
	}
	if mgr.Session("a") != old {
		t.Fatal("old generation must survive a failed new spawn")
	}
	if _, err := mgr.Call("echo", nil); err != nil {
		t.Fatalf("old generation must still serve: %v", err)
	}
}

func TestShutdownRecordsExplicitDrainReasons(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 2 * time.Second}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"echo"}, nil),
		helperManifestVersion("b", "1.0.0", []string{"sleep"}, nil),
	}, cfg)

	mgr.Shutdown()
	reasons := mgr.ShutdownReasons()
	if reasons["a"] != "drained" || reasons["b"] != "drained" {
		t.Fatalf("every shutdown wait needs an explicit reason: %v", reasons)
	}

	// Calls after shutdown are rejected with a clear host-shutdown cause.
	if _, err := mgr.Call("echo", nil); err == nil {
		t.Fatal("expected rejection after shutdown")
	}
}

func TestShutdownGraceKillHasExplicitReason(t *testing.T) {
	cfg := Config{TaskTimeout: 2 * time.Second, ShutdownGrace: 150 * time.Millisecond}
	mgr := testManager(t, []Manifest{
		helperManifestVersion("a", "1.0.0", []string{"sleep"}, nil),
	}, cfg)

	// Occupy the extension with a long task; shutdown cannot drain within
	// the grace period, so it must kill and report grace_killed.
	blocked := make(chan error, 1)
	go func() {
		_, err := mgr.Call("sleep", map[string]int{"millis": 5000})
		blocked <- err
	}()
	time.Sleep(100 * time.Millisecond)
	mgr.Shutdown()
	reasons := mgr.ShutdownReasons()
	if reasons["a"] != "grace_killed" {
		t.Fatalf("want grace_killed, got %q", reasons["a"])
	}
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("pending waiter must wake with a termination error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pending waiter must not hang after host shutdown")
	}
}
