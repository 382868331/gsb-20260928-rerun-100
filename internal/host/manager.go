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

	mu        sync.Mutex
	instances map[string]*Instance
	providers map[string]string // capability -> extension name
	taskSeq   int
	closed    bool
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
	if err := m.resolveOrder(); err != nil {
		return nil, err
	}
	return m, nil
}

// resolveOrder topologically sorts extensions so providers start before
// the extensions that require their capabilities.
func (m *Manager) resolveOrder() error {
	provider := map[string]string{} // capability -> owning extension
	for name, mf := range m.manifests {
		for _, c := range mf.Provides {
			if other, ok := provider[c]; ok {
				return fmt.Errorf("capability %q provided by both %q and %q", c, other, name)
			}
			provider[c] = name
		}
	}
	var order []string
	state := map[string]int{} // 0 unseen, 1 on-stack, 2 done
	var visit func(name string) error
	visit = func(name string) error {
		switch state[name] {
		case 1:
			return fmt.Errorf("dependency cycle through extension %q", name)
		case 2:
			return nil
		}
		state[name] = 1
		mf := m.manifests[name]
		for _, req := range mf.Requires {
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
	// Deterministic traversal: visit manifests in name order.
	names := make([]string, 0, len(m.manifests))
	for name := range m.manifests {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := visit(name); err != nil {
			return err
		}
	}
	m.order = order
	return nil
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
		if inst.name != name {
			inst.shutdown(0)
			m.rollback(started)
			return fmt.Errorf("start %s: binary identified itself as %q", name, inst.name)
		}
		// Capability compatibility check before becoming ready:
		// every declared requirement must be live, and every
		// advertised capability must be in the manifest.
		declared := map[string]bool{}
		for _, c := range mf.Provides {
			declared[c] = true
			if _, ok := inst.caps[c]; !ok {
				inst.shutdown(0)
				m.rollback(started)
				return fmt.Errorf("extension %s is incompatible: missing capability %q", name, c)
			}
		}
		for c := range inst.caps {
			if !declared[c] {
				inst.shutdown(0)
				m.rollback(started)
				return fmt.Errorf("extension %s is incompatible: undeclared capability %q", name, c)
			}
		}
		for _, req := range mf.Requires {
			if _, ok := m.providers[req]; !ok {
				inst.shutdown(0)
				m.rollback(started)
				return fmt.Errorf("extension %s requires capability %q which is not live", name, req)
			}
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
		if errors.Is(err, ErrCrashed) {
			entry.Status = "crashed"
		} else {
			entry.Status = "timeout"
		}
		entry.Detail = err.Error()
		m.record(entry)
		return nil, err
	}
	if resp.Type == protocol.TypeError {
		if resp.Err == ErrCrashed.Error() {
			entry.Status = "crashed"
		} else {
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
	return ok && inst.alive
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
		old.shutdown(m.cfg.ShutdownGrace)
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
	m.mu.Unlock()

	for i := len(m.order) - 1; i >= 0; i-- {
		name := m.order[i]
		m.mu.Lock()
		inst := m.instances[name]
		m.mu.Unlock()
		if inst != nil {
			inst.shutdown(m.cfg.ShutdownGrace)
		}
	}
}

func (m *Manager) record(e JournalEntry) {
	if m.cfg.Journal != nil {
		m.cfg.Journal.Record(e)
	}
}
