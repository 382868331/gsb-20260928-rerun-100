// upgrade-demo runs real extension subprocesses through dependency
// upgrades: old sessions keep serving while candidates prepare, traffic
// moves only after every candidate is confirmed, a failed candidate
// keeps the old combination, and each request's serving version/session
// is verifiable in the journal.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"extsys/internal/host"
)

func main() {
	binDir := flag.String("bin", "bin", "directory containing helper binaries")
	stateDir := flag.String("state", "state/upgrade-demo", "journal output directory")
	flag.Parse()
	if err := run(*binDir, *stateDir); err != nil {
		log.Fatalf("demo failed: %v", err)
	}
}

func run(binDir, stateDir string) error {
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		return err
	}

	// Compile four extension builds from the same trusted sources.
	// All are local pre-built artifacts; the version is stamped via an
	// environment variable baked into the launch manifest.
	buildDir := filepath.Join(stateDir, "builds")
	if err := os.MkdirAll(buildDir, 0o755); err != nil {
		return err
	}
	variants := []struct{ src, out string }{
		{"cmd/textkit", "textkit-v1"},
		{"cmd/textkit", "textkit-v2"},
		{"cmd/textkit", "textkit-v2-slow"},
		{"cmd/textkit", "textkit-v3"},
		{"cmd/hashkit", "hashkit-v1"},
		{"cmd/hashkit", "hashkit-v2"},
	}
	for _, v := range variants {
		fmt.Printf("[build] %s -> %s\n", v.src, v.out)
		if err := buildExtension(filepath.Join(buildDir, v.out), v.src); err != nil {
			return err
		}
	}
	// A helper binary that refuses the handshake, built from a tiny
	// local source below.
	reject := filepath.Join(binDir, "hashkit-reject")
	if err := os.WriteFile(filepath.Join(stateDir, "reject-main.go"), []byte(rejectSrc), 0o644); err != nil {
		return err
	}
	if err := buildModule(reject, filepath.Join(stateDir, "reject-main.go")); err != nil {
		return err
	}

	journal, err := host.OpenJournal(filepath.Join(stateDir, "switch.jsonl"))
	if err != nil {
		return err
	}
	defer journal.Close()

	cfg := host.Config{
		HandshakeTimeout: 3 * time.Second,
		TaskTimeout:      2 * time.Second,
		ShutdownGrace:    1 * time.Second,
		Journal:          journal,
	}

	manifests := []host.Manifest{{
		Name:     "hashkit",
		Path:     filepath.Join(buildDir, "hashkit-v1"),
		Env:      []string{"HASHKIT_VERSION=0.9.1"},
		Provides: []string{"sha256", "reverse", "hashwho"},
	}, {
		Name:     "textkit",
		Path:     filepath.Join(buildDir, "textkit-v1"),
		Env:      []string{"TEXTKIT_VERSION=1.2.0"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic", "textwho"},
		Requires: []string{"sha256"},
	}}

	mgr, err := host.New(manifests, cfg)
	if err != nil {
		return err
	}
	if err := mgr.Start(); err != nil {
		return err
	}
	defer mgr.Shutdown()
	fmt.Println("[host] v1 combination live: hashkit 0.9.1 + textkit 1.2.0")

	mustCall := func(capName string, payload any) string {
		raw, err := mgr.Call(capName, payload)
		if err != nil {
			return "ERROR: " + err.Error()
		}
		return mustJSON(raw)
	}

	fmt.Println("[task v1] uppercase ->", mustCall("uppercase", map[string]string{"text": "before upgrade"}))
	v1Hash := mgr.Session("hashkit")
	v1Text := mgr.Session("textkit")
	fmt.Printf("[session] v1 hashkit=%s... textkit=%s...\n", v1Hash[:8], v1Text[:8])

	// Scenario 1: a dependency candidate refuses the handshake.
	fmt.Println()
	fmt.Println("=== Scenario 1: hashkit v2 candidate refuses the handshake ===")
	bad := []host.Manifest{{
		Name:     "hashkit",
		Path:     reject,
		Provides: []string{"sha256", "reverse", "hashwho"},
	}, {
		Name:     "textkit",
		Path:     filepath.Join(buildDir, "textkit-v2"),
		Env:      []string{"TEXTKIT_VERSION=2.0.0"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic", "textwho"},
		Requires: []string{"sha256"},
	}}
	if err := mgr.Upgrade(bad); err != nil {
		fmt.Printf("[upgrade] rejected as expected: %v\n", err)
	}
	fmt.Println("[task after-fail] uppercase ->", mustCall("uppercase", map[string]string{"text": "still old combo"}))
	fmt.Printf("[check] hashkit still %s session %s... (unchanged: %v)\n",
		mgr.Version("hashkit"), shortSess(mgr.Session("hashkit")), mgr.Session("hashkit") == v1Hash)
	fmt.Printf("[check] textkit still %s session %s... (unchanged: %v)\n",
		mgr.Version("textkit"), shortSess(mgr.Session("textkit")), mgr.Session("textkit") == v1Text)

	// Scenario 2: a successful dependency upgrade, fresh sessions.
	// The new textkit is a build that takes 4s on its own shutdown;
	// that delay only matters later, when this build is retired.
	fmt.Println()
	fmt.Println("=== Scenario 2: upgrade hashkit->v2 + textkit->2.0.0-slow ===")
	good := []host.Manifest{{
		Name:     "hashkit",
		Path:     filepath.Join(buildDir, "hashkit-v2"),
		Env:      []string{"HASHKIT_VERSION=2.0.0"},
		Provides: []string{"sha256", "reverse", "hashwho"},
	}, {
		Name:     "textkit",
		Path:     filepath.Join(buildDir, "textkit-v2-slow"),
		Env:      []string{"TEXTKIT_VERSION=2.0.0-slow", "TEXTKIT_SHUTDOWN_DELAY_MS=4000"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic", "textwho"},
		Requires: []string{"sha256"},
	}}
	if err := mgr.Upgrade(good); err != nil {
		return fmt.Errorf("scenario 2: %w", err)
	}
	v2Hash := mgr.Session("hashkit")
	v2Text := mgr.Session("textkit")
	fmt.Println("[task v2] uppercase ->", mustCall("uppercase", map[string]string{"text": "after upgrade"}))
	fmt.Printf("[check] hashkit %s session %s... ; textkit %s session %s...\n",
		mgr.Version("hashkit"), shortSess(v2Hash), mgr.Version("textkit"), shortSess(v2Text))
	if v2Hash == v1Hash || v2Text == v1Text {
		return fmt.Errorf("session identity leaked across upgrade")
	}

	// Scenario 3: a consecutive upgrade whose old session overruns the
	// agreed drain boundary. Traffic still moves, but the host must not
	// claim the migration was fully clean.
	fmt.Println()
	fmt.Println("=== Scenario 3: consecutive upgrade to textkit v3; old slow build is killed ===")
	v3 := []host.Manifest{{
		Name:     "textkit",
		Path:     filepath.Join(buildDir, "textkit-v3"),
		Env:      []string{"TEXTKIT_VERSION=3.0.0"},
		Provides: []string{"uppercase", "wordcount", "sleep", "panic", "textwho"},
		Requires: []string{"sha256"},
	}}
	err = mgr.Upgrade(v3)
	if err == nil {
		return fmt.Errorf("expected incomplete-handoff error from slow drain")
	}
	fmt.Printf("[upgrade] cut over, reported honestly: %v\n", err)
	fmt.Println("[task v3] uppercase ->", mustCall("uppercase", map[string]string{"text": "new build serves"}))
	v3Text := mgr.Session("textkit")
	fmt.Printf("[check] textkit now %s session %s... (distinct from v1/v2: %v)\n",
		mgr.Version("textkit"), shortSess(v3Text), v3Text != v2Text && v3Text != v1Text)

	fmt.Println()
	fmt.Println("=== Switch audit trail (state/upgrade-demo/switch.jsonl) ===")
	for _, rec := range journal.SwitchRecords() {
		fmt.Printf("%-10s %-18s ext=%-8s %s -> %s  %s\n",
			rec.UpgradeID, rec.Event, dash(rec.Extension), dash(rec.OldVersion), dash(rec.NewVersion), rec.Detail)
	}
	fmt.Println()
	fmt.Println("Each task record in switch.jsonl also names the version+session that served it.")
	return nil
}

const rejectSrc = `package main

import (
	"encoding/json"
	"os"
)

func main() {
	var hello map[string]any
	_ = json.NewDecoder(os.Stdin).Decode(&hello)
	_ = json.NewEncoder(os.Stdout).Encode(map[string]any{
		"type":  "error",
		"error": "handshake refused by simulated broken build",
	})
	os.Exit(1)
}
`

func buildExtension(out, pkg string) error {
	cmd := exec.Command("go", "build", "-o", out, "./"+pkg)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// buildModule compiles a standalone single-file program outside the
// module (no internal imports), representing a broken candidate build.
func buildModule(out, src string) error {
	cmd := exec.Command("go", "build", "-o", out, src)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

func mustJSON(raw json.RawMessage) string {
	var v any
	if json.Unmarshal(raw, &v) != nil {
		return string(raw)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func shortSess(s string) string {
	if len(s) < 8 {
		return s
	}
	return s[:8]
}

func dash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "-"
	}
	return s
}
