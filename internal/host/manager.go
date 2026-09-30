// Package host implements the application side of the extension system:
// manifest validation, dependency-ordered startup, supervised dispatch
// with timeouts, crash isolation, and graceful shutdown.
package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"extsys/internal/protocol"
)

// Manifest describes an extension the application may load. Binaries are
// local, trusted build artifacts only; nothing is downloaded or fetched.
type Manifest struct {
	Name string
	Path string
	// Args and Env are optional process overrides, mainly for tests.
	Args []string
	Env  []string
	// Provides is the static declaration of capabilities this
	// extension implements, used to order startup. The handshake
	// re-verifies that the running binary actually offers them.
	Provides []string
	// Requires lists capabilities provided by other extensions that
	// must be live before this extension may start.
	Requires []string
}

// Config tunes manager behaviour.
type Config struct {
	HandshakeTimeout time.Duration
	TaskTimeout      time.Duration
	ShutdownGrace    time.Duration
	Journal          *Journal
}

// Manager owns the lifecycle of all extension processes.
type Manager struct {
	cfg       Config
	manifests map[string]Manifest
	order     []string // dependency-resolved start order

	mu        sync.Mutex // held for the duration of a handover
	instances map[string]*Instance
	providers map[string]string // capability -> extension name
	taskSeq   int
	closed    bool

	shutdownReasons map[string]string
}

// New validates manifests and resolves the dependency order. Any
// missing requirement or dependency cycle fails here, before anything
// runs.
func New(manifests []Manifest, cfg Config) (*Manager, error) {
	m := &Manager{
		cfg:       cfg,
		manifests: map[string]Manifest{},
		instances: map[string]*Instance{},
		providers: map[string]string{},
	}
	for _, mf := range manifests {
		if mf.Name == "" || mf.Path == "" {
			return nil, fmt.Errorf("manifest with empty name or path")
		}
		if _, dup := m.manifests[mf.Name]; dup {
			return nil, fmt.Errorf("duplicate extension %q", mf.Name)
		}
		m.manifests[mf.Name] = mf
	}
	order, err := resolveOrder(m.manifests)
	if err != nil {
		return nil, err
	}
	m.order = order
	return m, nil
}

// Start launches extensions in dependency order and verifies the
// handshake: protocol version, session token, and the advertised
// capabilities must match the manifest. A failed check aborts startup
// and rolls back everything already launched.
func (m *Manager) Start() error {
	var started []string
	for _, name := range m.order {
		mf := m.manifests[name]
		inst, err := spawn(mf, m.cfg.HandshakeTimeout)
		if err != nil {
			m.rollback(started)
			return fmt.Errorf("start %s: %w", name, err)
		}
		if err := m.verifyInstance(mf, inst, nil); err != nil {
			inst.shutdownNow()
			m.rollback(started)
			return fmt.Errorf("start %s: %w", name, err)
		}
		m.instances[name] = inst
		for _, c := range mf.Provides {
			m.providers[c] = name
		}
		started = append(started, name)
	}
	return nil
}

func (m *Manager) rollback(names []string) {
	for i := len(names) - 1; i >= 0; i-- {
		if inst, ok := m.instances[names[i]]; ok {
			inst.shutdown(m.cfg.ShutdownGrace)
			delete(m.instances, names[i])
		}
	}
}

// SwitchResult describes how one extension's old generation ended.
type SwitchResult struct {
	Name        string
	OldVersion  string
	OldSession  string
	NewVersion  string
	NewSession  string
	DrainReason string
}

// UpgradeSet performs an atomic handover of the named extensions to new
// manifests (new binaries/versions). Dependencies that are themselves
// upgrading are validated as a set: every new process starts and
// completes identity/capability/dependency verification BEFORE any task
// routing changes. Only after all new generations are healthy does the
// manager commit routing; new tasks then go exclusively to the new
// generations, while each old process keeps serving the tasks it
// already accepted, drains to a boundary (bye or grace), and exits.
//
// If any new process fails to start or verify, all freshly spawned
// processes are discarded and the old generations remain the sole task
// owners. A task is never dispatched to two generations.
func (m *Manager) UpgradeSet(updates []Manifest) ([]SwitchResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, fmt.Errorf("manager is shutting down")
	}

	upgraded := map[string]bool{}
	futureManifests := map[string]Manifest{}
	for name, mf := range m.manifests {
		futureManifests[name] = mf
	}
	for _, mf := range updates {
		if mf.Name == "" || mf.Path == "" {
			return nil, fmt.Errorf("manifest with empty name or path")
		}
		if _, known := futureManifests[mf.Name]; !known {
			return nil, fmt.Errorf("cannot upgrade unknown extension %q", mf.Name)
		}
		if upgraded[mf.Name] {
			return nil, fmt.Errorf("duplicate upgrade for extension %q", mf.Name)
		}
		upgraded[mf.Name] = true
		futureManifests[mf.Name] = mf
	}

	// Statically re-validate the target set: unique providers, no
	// missing requirements, no cycles among the combined manifests.
	futureOrder, err := resolveOrder(futureManifests)
	if err != nil {
		m.recordSwitchFailures(upgraded, "rejected: "+err.Error())
		return nil, err
	}

	// Phase 1: start and verify every new generation while the old ones
	// keep serving. Nothing is routable yet.
	newInsts := map[string]*Instance{}
	names := make([]string, 0, len(updates))
	for _, name := range futureOrder {
		if !upgraded[name] {
			continue
		}
		names = append(names, name)
		mf := futureManifests[name]
		inst, err := spawn(mf, m.cfg.HandshakeTimeout)
		if err != nil {
			m.abortUpgrade(names, newInsts, upgraded, "spawn: "+err.Error())
			return nil, fmt.Errorf("upgrade %s: %w", name, err)
		}
		if err := m.verifyInstance(mf, inst, newInsts); err != nil {
			inst.shutdownNow()
			m.abortUpgrade(names, newInsts, upgraded, err.Error())
			return nil, fmt.Errorf("upgrade %s: %w", name, err)
		}
		newInsts[name] = inst
	}

	// Phase 2: atomic commit. Build the target routing tables first and
	// publish them together; old generations are detached but running.
	oldInsts := make(map[string]*Instance, len(names))
	results := make([]SwitchResult, 0, len(names))
	for _, name := range names {
		old := m.instances[name]
		oldInsts[name] = old
		mf := futureManifests[name]
		inst := newInsts[name]
		res := SwitchResult{Name: name, NewVersion: inst.extVer, NewSession: inst.session}
		if old != nil {
			res.OldVersion = old.extVer
			res.OldSession = old.session
		}
		results = append(results, res)
		m.manifests[name] = mf
		m.instances[name] = inst
		for _, c := range mf.Provides {
			m.providers[c] = name
		}
	}
	m.order = futureOrder

	// Phase 3: drain and stop old generations, in reverse dependency
	// order (dependents stop before their providers). New tasks are
	// already owned solely by the new generations because this method
	// holds m.mu for its whole duration, so no Call can route to an old
	// process concurrently.
	for i := len(results) - 1; i >= 0; i-- {
		name := results[i].Name
		old := oldInsts[name]
		reason := "absent"
		if old != nil {
			reason = old.terminate(m.cfg.ShutdownGrace, ErrRetired)
		}
		results[i].DrainReason = reason
		m.recordSwitch(SwitchEntry{
			Extension:  name,
			Action:     "upgraded",
			Result:     "committed",
			OldVersion: results[i].OldVersion,
			OldSession: results[i].OldSession,
			NewVersion: results[i].NewVersion,
			NewSession: results[i].NewSession,
			Drain:      reason,
		})
	}
	return results, nil
}

// verifyInstance applies the identity, capability and liveness checks
// to a freshly spawned instance. liveNew holds other generations that
// are upgrading in the same set; dependencies are considered live if
// they are either an already-verified new generation or an untouched
// existing instance.
func (m *Manager) verifyInstance(mf Manifest, inst *Instance, liveNew map[string]*Instance) error {
	if inst.name != mf.Name {
		return fmt.Errorf("binary identified itself as %q", inst.name)
	}
	declared := map[string]bool{}
	for _, c := range mf.Provides {
		declared[c] = true
		if _, ok := inst.caps[c]; !ok {
			return fmt.Errorf("incompatible: missing capability %q", c)
		}
	}
	for c := range inst.caps {
		if !declared[c] {
			return fmt.Errorf("incompatible: undeclared capability %q", c)
		}
	}
	for _, req := range mf.Requires {
		provider, ok := m.providers[req]
		if !ok {
			return fmt.Errorf("requires capability %q which is not live", req)
		}
		if _, fresh := liveNew[provider]; !fresh {
			if existing := m.instances[provider]; existing == nil {
				return fmt.Errorf("requires capability %q whose provider is not live", req)
			}
		}
	}
	return nil
}

// abortUpgrade discards freshly spawned generations and records that
// the old versions were retained.
func (m *Manager) abortUpgrade(names []string, fresh map[string]*Instance, upgraded map[string]bool, detail string) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		if inst, ok := fresh[name]; ok {
			inst.shutdownNow()
		}
	}
	m.recordSwitchFailures(upgraded, detail)
}

func (m *Manager) recordSwitchFailures(upgraded map[string]bool, detail string) {
	for name := range upgraded {
		old := m.instances[name]
		e := SwitchEntry{Extension: name, Action: "upgrade_failed", Result: "retained_old", Detail: detail}
		if old != nil {
			e.OldVersion = old.extVer
			e.OldSession = old.session
		}
		m.recordSwitch(e)
	}
}

// resolveOrder topologically sorts manifests so providers start before
// the extensions that require them.
func resolveOrder(manifests map[string]Manifest) ([]string, error) {
	provider := map[string]string{}
	for name, mf := range manifests {
		for _, c := range mf.Provides {
			if other, ok := provider[c]; ok {
				return nil, fmt.Errorf("capability %q provided by both %q and %q", c, other, name)
			}
			provider[c] = name
		}
	}
	var order []string
	state := map[string]int{}
	var visit func(string) error
	visit = func(name string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("dependency cycle through extension %q", name)
		case 2:
			return nil
		}
		state[name] = 1
		for _, req := range manifests[name].Requires {
			other, ok := provider[req]
			if !ok {
				return fmt.Errorf("extension %s requires capability %q which no extension provides", name, req)
			}
			if other == name {
				return fmt.Errorf("extension %s cannot provide its own requirement %q", name, req)
			}
			if err := visit(other); err != nil {
				return err
			}
		}
		state[name] = 2
		order = append(order, name)
		return nil
	}
	names := make([]string, 0, len(manifests))
	for name := range manifests {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return nil, err
		}
	}
	return order, nil
}

// Call dispatches one task to the extension providing the capability.
// A crash or timeout only fails this call; other extensions and their
// in-flight requests are unaffected. Every attempt is journaled.
func (m *Manager) Call(capability string, payload any) (json.RawMessage, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("manager is shutting down")
	}
	m.taskSeq++
	id := fmt.Sprintf("task-%d", m.taskSeq)
	extName, ok := m.providers[capability]
	inst := m.instances[extName]
	m.mu.Unlock()

	entry := JournalEntry{TaskID: id, Capability: capability, Extension: extName}
	if !ok || inst == nil {
		entry.Status = "rejected"
		entry.Detail = "no provider"
		m.record(entry)
		return nil, fmt.Errorf("no extension provides capability %q", capability)
	}
	entry.ExtVersion = inst.extVer
	entry.Session = inst.session

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	resp, err := inst.call(id, capability, raw, m.cfg.TaskTimeout)
	if err != nil {
		switch {
		case errors.Is(err, ErrCrashed):
			entry.Status = "crashed"
		case errors.Is(err, ErrRetired):
			entry.Status = "retired"
		case errors.Is(err, ErrShutdown):
			entry.Status = "shutdown"
		default:
			entry.Status = "timeout"
		}
		entry.Detail = err.Error()
		m.record(entry)
		return nil, err
	}
	if resp.Type == protocol.TypeError {
		switch resp.Err {
		case ErrCrashed.Error():
			entry.Status = "crashed"
		case ErrRetired.Error():
			entry.Status = "retired"
		case ErrShutdown.Error():
			entry.Status = "shutdown"
		default:
			entry.Status = "error"
		}
		entry.Detail = resp.Err
		m.record(entry)
		return nil, fmt.Errorf("%s: %s", extName, resp.Err)
	}
	entry.Status = "ok"
	m.record(entry)
	return resp.Payload, nil
}

// Alive reports whether the extension process is still running.
func (m *Manager) Alive(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	inst, ok := m.instances[name]
	return ok && inst.isAlive()
}

// Restart replaces a dead extension process with a fresh one. The new
// process gets a new session token; the old session is never reused, so
// a stale process cannot impersonate the previous incarnation.
func (m *Manager) Restart(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mf, ok := m.manifests[name]
	if !ok {
		return fmt.Errorf("unknown extension %q", name)
	}
	if old, ok := m.instances[name]; ok {
		for capName, provider := range m.providers {
			if provider == name {
				delete(m.providers, capName)
			}
		}
		old.terminate(m.cfg.ShutdownGrace, ErrRetired)
		delete(m.instances, name)
	}
	inst, err := spawn(mf, m.cfg.HandshakeTimeout)
	if err != nil {
		return err
	}
	m.instances[name] = inst
	for _, c := range mf.Provides {
		m.providers[c] = name
	}
	return nil
}

// Session returns the current session token of an extension.
func (m *Manager) Session(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst, ok := m.instances[name]; ok {
		return inst.session
	}
	return ""
}

// Shutdown stops accepting new calls, then shuts extensions down in
// reverse start order: each drains and exits voluntarily and is killed
// only if it exceeds the grace period.
func (m *Manager) Shutdown() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	reasons := map[string]string{}
	m.shutdownReasons = reasons

	for i := len(m.order) - 1; i >= 0; i-- {
		name := m.order[i]
		inst := m.instances[name]
		if inst != nil {
			reason := inst.terminate(m.cfg.ShutdownGrace, ErrShutdown)
			reasons[name] = reason
			result := "drained"
			if reason == "grace_killed" {
				result = "grace_killed"
			} else if reason == "exited" || reason == "already_exited" {
				result = "exited"
			}
			m.recordSwitch(SwitchEntry{
				Extension:  name,
				Action:     "shutdown",
				Result:     result,
				OldVersion: inst.extVer,
				OldSession: inst.session,
				Drain:      reason,
			})
		}
	}
	m.mu.Unlock()
}

// ShutdownReasons reports the explicit termination reason recorded for
// each extension at host shutdown (drained | exited | grace_killed |
// already_exited | absent).
func (m *Manager) ShutdownReasons() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]string{}
	for k, v := range m.shutdownReasons {
		out[k] = v
	}
	return out
}

func (m *Manager) record(e JournalEntry) {
	if m.cfg.Journal != nil {
		m.cfg.Journal.Record(e)
	}
}

func (m *Manager) recordSwitch(e SwitchEntry) {
	if m.cfg.Journal != nil {
		m.cfg.Journal.RecordSwitch(e)
	}
}
