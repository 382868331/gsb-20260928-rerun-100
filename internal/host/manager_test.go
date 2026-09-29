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
	caps := map[string]bool{}
	for _, c := range strings.Split(os.Getenv("EXTSYS_TEST_CAPS"), ",") {
		if c != "" {
			caps[c] = true
		}
	}
	ext := extsdk.New(name, "3.1.4-test")
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
