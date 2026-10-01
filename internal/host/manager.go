// Package host implements the application side of the extension system:
// manifest validation, dependency-ordered startup, supervised dispatch
// with timeouts, crash isolation, and graceful shutdown.
package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
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
	switchSeq int
	closed    bool
	stopCh    chan struct{} // closed when host shutdown begins
	retired   []*Instance   // old incarnations draining in flight

	upgradeMu sync.Mutex // serialises overlapping upgrade attempts
	upgrading sync.WaitGroup
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
	order, err := validateManifests(manifests, m.manifests)
	if err != nil {
		return nil, err
	}
	m.order = order
	return m, nil
}

// validateManifests checks the candidate set for basic problems,
// duplicate capability providers and dependency cycles, then returns
// the dependency-resolved start order. existing is used when verifying
// candidate sets during an upgrade (its contents are overwritten).
func validateManifests(manifests []Manifest, existing map[string]Manifest) ([]string, error) {
	for _, mf := range manifests {
		if mf.Name == "" || mf.Path == "" {
			return nil, fmt.Errorf("manifest with empty name or path")
		}
		if _, dup := existing[mf.Name]; dup {
			return nil, fmt.Errorf("duplicate extension %q", mf.Name)
		}
		existing[mf.Name] = mf
	}
	return resolveOrder(existing)
}

// resolveOrder topologically sorts extensions so providers start before
// the extensions that require their capabilities.
func resolveOrder(manifests map[string]Manifest) ([]string, error) {
	provider := map[string]string{} // capability -> owning extension
	for name, mf := range manifests {
		for _, c := range mf.Provides {
			if other, ok := provider[c]; ok {
				return nil, fmt.Errorf("capability %q provided by both %q and %q", c, other, name)
			}
			provider[c] = name
		}
	}
	var order []string
	state := map[string]int{} // 0 unseen, 1 on-stack, 2 done
	var visit func(name string) error
	visit = func(name string) error {
		mf, ok := manifests[name]
		if !ok {
			return fmt.Errorf("unknown extension %q", name)
		}
		switch state[name] {
		case 1:
			return fmt.Errorf("dependency cycle through extension %q", name)
		case 2:
			return nil
		}
		state[name] = 1
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
		if err := m.verifyInstance(mf, inst, m.providers); err != nil {
			inst.shutdown(0)
			m.rollback(started)
			return err
		}
		m.instances[name] = inst
		for _, c := range mf.Provides {
			m.providers[c] = name
		}
		started = append(started, name)
	}
	return nil
}

// verifyInstance checks a freshly spawned candidate before it is
// allowed to receive a single task: identity, exact capability set,
// and that every required capability is provided by some (already
// ordered) extension. live maps capability -> provider within the set
// being prepared; nil falls back to the running provider table.
func (m *Manager) verifyInstance(mf Manifest, inst *Instance, providers map[string]string) error {
	name := mf.Name
	if inst.name != name {
		return fmt.Errorf("start %s: binary identified itself as %q", name, inst.name)
	}
	declared := map[string]bool{}
	for _, c := range mf.Provides {
		declared[c] = true
		if _, ok := inst.caps[c]; !ok {
			return fmt.Errorf("extension %s is incompatible: missing capability %q", name, c)
		}
	}
	for c := range inst.caps {
		if !declared[c] {
			return fmt.Errorf("extension %s is incompatible: undeclared capability %q", name, c)
		}
	}
	for _, req := range mf.Requires {
		if _, ok := providers[req]; !ok {
			return fmt.Errorf("extension %s requires capability %q which is not live", name, req)
		}
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
	inst, ok := m.instances[name]
	m.mu.Unlock()
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

// stagedItem is one extension in an in-progress upgrade.
type stagedItem struct {
	name string
	new  *Instance // nil until spawned
	old  *Instance // nil when there was no running incarnation
}

// Upgrade atomically replaces one or more extension builds. Candidates
// are spawned and fully validated (protocol handshake, identity,
// capability set, dependencies against the candidate provider table)
// before any traffic is cut over, so a failing candidate keeps the old
// version serving untouched. Only after cutover does each old process
// stop receiving new tasks and drain the work it already accepted.
// Every phase is appended to the switch audit trail.
//
// This makes the combination "dependency also upgrading + old process
// exits + new version fails to start" safe: a failure anywhere before
// cutover tears down only the candidates, while the running versions
// (including any that crashed independently) remain the task owners.
//
// Session identities never carry over: each spawned candidate gets a
// fresh random token, so two consecutive upgrades, or a retry after a
// failed attempt, can never reuse an old session. The returned error is
// nil only when cutover succeeded AND every retired process ended for
// the expected reason (clean shutdown within grace). If some old session
// exited on its own, had to be killed past the drain boundary, or was
// killed because the host closed, Upgrade returns an error describing
// the real end reasons and records "upgrade_incomplete": traffic has
// already moved to the new builds, but the handoff was not fully clean.
func (m *Manager) Upgrade(candidates []Manifest) error {
	m.upgradeMu.Lock()
	m.upgrading.Add(1)
	defer m.upgrading.Done()
	defer m.upgradeMu.Unlock()

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fmt.Errorf("manager is shutting down")
	}
	m.switchSeq++
	upgradeID := fmt.Sprintf("upgrade-%d", m.switchSeq)
	stop := m.stopLocked()
	m.mu.Unlock()
	if len(candidates) == 0 {
		return fmt.Errorf("upgrade requires at least one candidate")
	}

	// Build the post-upgrade manifest set: candidates replace existing
	// manifests with the same name; other extensions are kept.
	next := map[string]Manifest{}
	for name, mf := range m.manifests {
		next[name] = mf
	}
	seen := map[string]bool{}
	for _, mf := range candidates {
		if mf.Name == "" || mf.Path == "" {
			return fmt.Errorf("candidate with empty name or path")
		}
		if seen[mf.Name] {
			return fmt.Errorf("duplicate candidate %q", mf.Name)
		}
		seen[mf.Name] = true
		if _, known := next[mf.Name]; !known {
			return fmt.Errorf("upgrade unknown extension %q", mf.Name)
		}
		next[mf.Name] = mf
	}
	order, err := resolveOrder(next)
	if err != nil {
		for _, mf := range candidates {
			m.recordSwitch(SwitchAbort, upgradeID, mf.Name, m.instances[mf.Name], nil,
				"candidate set rejected by topology check: "+err.Error())
		}
		return err
	}

	// Snapshot current instances/providers for dependency checks and
	// for restoring ownership if the upgrade must be abandoned.
	m.mu.Lock()
	liveProviders := map[string]string{}
	for c, p := range m.providers {
		liveProviders[c] = p
	}
	staged := make([]stagedItem, 0, len(candidates))
	byName := map[string]*stagedItem{}
	for _, mf := range candidates {
		staged = append(staged, stagedItem{name: mf.Name, old: m.instances[mf.Name]})
		byName[mf.Name] = &staged[len(staged)-1]
	}
	m.mu.Unlock()

	m.recordSwitch(SwitchValidate, upgradeID, "", nil, nil, "candidate set passed topology checks")

	// Spawn candidates in dependency order. The candidate provider
	// table starts from live providers, then overlays each candidate as
	// it is prepared, so a candidate requiring an upgraded dependency
	// is validated against the new build, not the old one.
	candidateProviders := map[string]string{}
	for c, p := range liveProviders {
		candidateProviders[c] = p
	}
	for _, mf := range candidates {
		for _, c := range mf.Provides {
			candidateProviders[c] = mf.Name
		}
	}

	for _, name := range order {
		item := byName[name]
		if item == nil {
			continue
		}
		mf := next[name]
		inst, spawnErr := m.spawnCandidate(mf, stop)
		if spawnErr != nil {
			m.abortUpgrade(upgradeID, staged, spawnErr, stop)
			return spawnErr
		}
		if err := m.verifyInstance(mf, inst, candidateProviders); err != nil {
			inst.killNow()
			m.abortUpgrade(upgradeID, staged, err, stop)
			return err
		}
		item.new = inst
		m.recordSwitch(SwitchPrepared, upgradeID, name, item.old, inst,
			"candidate confirmed: handshake, identity and capability checks passed")
	}

	// All candidates are valid. Cut over atomically: new tasks now go
	// only to the new instances. Calls already in flight keep holding
	// the old Instance pointer, so a task is never delivered twice.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		closeErr := fmt.Errorf("host closed before cutover")
		m.abortUpgrade(upgradeID, staged, closeErr, stop)
		return closeErr
	}
	for i := range staged {
		item := &staged[i]
		if item.old != nil {
			m.retired = append(m.retired, item.old)
		}
		m.instances[item.name] = item.new
		m.manifests[item.name] = next[item.name]
		m.recordSwitch(SwitchCutover, upgradeID, item.name, item.old, item.new,
			"all candidates confirmed; new tasks now route to new version")
	}
	m.order = order
	m.mu.Unlock()

	// Drain old incarnations in reverse dependency order so providers
	// outlive their dependents during the handoff. Every drain ends for
	// an explicit, recorded reason. "migrated" is recorded per
	// extension only when its old session ended as agreed; a self-exit,
	// a grace kill or a host-close kill keeps the upgrade from being
	// declared fully complete, even though traffic is already on the
	// new builds.
	var badReasons []string
	for i := len(staged) - 1; i >= 0; i-- {
		item := &staged[i]
		old := item.old
		if old == nil {
			m.recordSwitch(SwitchMigrated, upgradeID, item.name, nil, item.new,
				"no previous incarnation; new version live")
			continue
		}
		reason := old.drain(m.cfg.ShutdownGrace, stop)
		event, detail := drainEvent(reason)
		m.recordSwitch(event, upgradeID, item.name, old, item.new, detail)
		m.mu.Lock()
		m.retired = removeInstance(m.retired, old)
		m.mu.Unlock()
		if reason == EndBye {
			m.recordSwitch(SwitchMigrated, upgradeID, item.name, old, item.new,
				"old session drained within the agreed boundary; new version live")
		} else {
			badReasons = append(badReasons, fmt.Sprintf("%s old session ended: %s (%s)", item.name, reason, detail))
		}
	}
	if len(badReasons) == 0 {
		m.recordSwitch(SwitchComplete, upgradeID, "", nil, nil, "every extension migrated cleanly")
		return nil
	}
	m.recordSwitch(SwitchIncomplete, upgradeID, "", nil, nil, strings.Join(badReasons, "; "))
	return fmt.Errorf("upgrade cut over, but handoff was not clean: %s", strings.Join(badReasons, "; "))
}

// drainEvent maps the observed end reason of a retired process to the
// audit event and a human-readable explanation.
func drainEvent(reason EndReason) (event, detail string) {
	switch reason {
	case EndBye:
		return SwitchDrained, "old process acknowledged shutdown and exited within grace"
	case EndExited:
		return SwitchDrainExited, "old process exited on its own during drain (no handshake close)"
	case EndGraceKill:
		return SwitchDrainKilled, "old process exceeded the agreed drain boundary; killed"
	case EndHostClose:
		return SwitchHostClose, "host closed during drain; old process killed"
	default:
		return string(reason), string(reason)
	}
}

// spawnCandidate spawns one candidate but aborts promptly with an
// explicit reason if host shutdown starts during the handshake.
func (m *Manager) spawnCandidate(mf Manifest, stop <-chan struct{}) (*Instance, error) {
	type result struct {
		inst *Instance
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		inst, err := spawn(mf, m.cfg.HandshakeTimeout)
		ch <- result{inst, err}
	}()
	select {
	case r := <-ch:
		return r.inst, r.err
	case <-stop:
		go func() {
			r := <-ch
			if r.inst != nil {
				r.inst.killNow()
			}
		}()
		return nil, fmt.Errorf("host closed while starting %s", mf.Name)
	}
}

// abortUpgrade tears down every prepared candidate in reverse order
// and records that the old version was retained. Candidates that never
// spawned are reported as cancelled. It never touches the running
// instances, so task ownership stays with the old build even when an
// old process exited independently during preparation.
func (m *Manager) abortUpgrade(upgradeID string, staged []stagedItem, cause error, stop <-chan struct{}) {
	for i := len(staged) - 1; i >= 0; i-- {
		item := &staged[i]
		cand := item.new
		if cand == nil {
			detail := fmt.Sprintf("candidate cancelled before cutover (%v)", cause)
			if item.old != nil {
				detail += "; old version still serving"
			}
			m.recordSwitch(SwitchAbort, upgradeID, item.name, item.old, nil, detail)
			continue
		}
		reason := cand.drain(m.cfg.ShutdownGrace, stop)
		_, drainDetail := drainEvent(reason)
		m.recordSwitch(SwitchRollback, upgradeID, item.name, item.old, cand,
			fmt.Sprintf("candidate rejected before cutover (%v); candidate removed: %s; old version retained", cause, drainDetail))
	}
}

func removeInstance(list []*Instance, target *Instance) []*Instance {
	out := list[:0]
	for _, inst := range list {
		if inst != target {
			out = append(out, inst)
		}
	}
	return out
}

func (m *Manager) recordSwitch(event, upgradeID, name string, old, new *Instance, detail string) {
	if m.cfg.Journal == nil {
		return
	}
	rec := SwitchRecord{Event: event, UpgradeID: upgradeID, Extension: name, Detail: detail}
	if old != nil {
		rec.OldVersion, rec.OldSession = old.extVer, old.session
	}
	if new != nil {
		rec.NewVersion, rec.NewSession = new.extVer, new.session
	}
	m.cfg.Journal.Switch(rec)
}

// stopLocked returns the shutdown signal channel, creating it.
func (m *Manager) stopLocked() <-chan struct{} {
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
	return m.stopCh
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

// Version returns the version string of the build currently serving
// an extension, as advertised in its handshake.
func (m *Manager) Version(name string) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if inst, ok := m.instances[name]; ok {
		return inst.extVer
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
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
	close(m.stopCh)
	m.mu.Unlock()

	// Wait for an in-progress upgrade to reach a stable point
	// (cutover or rollback) so no instance is supervised twice.
	m.upgrading.Wait()

	m.mu.Lock()
	order := append([]string{}, m.order...)
	live := make([]*Instance, 0, len(order))
	for _, name := range order {
		if inst := m.instances[name]; inst != nil {
			live = append(live, inst)
		}
	}
	retired := append([]*Instance{}, m.retired...)
	m.mu.Unlock()

	for i := len(live) - 1; i >= 0; i-- {
		live[i].shutdown(m.cfg.ShutdownGrace)
	}
	// Old incarnations still draining from a finished cutover also get
	// an explicit, grace-bounded stop.
	for _, inst := range retired {
		inst.killNow()
	}
}

func (m *Manager) record(e JournalEntry) {
	if m.cfg.Journal != nil {
		m.cfg.Journal.Record(e)
	}
}
