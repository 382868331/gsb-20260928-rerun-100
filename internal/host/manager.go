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

	owners map[string]SessionOwnership // session token -> responsible version
	subs   map[string]*subscription    // ext/sub -> authoritative event ledger
	plan   *UpgradePlan                // prepared upgrade awaiting confirmation

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
		owners:    map[string]SessionOwnership{},
		subs:      map[string]*subscription{},
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

// stop is closed during Shutdown so long-running upgrade preparations
// end with an explicit reason instead of blocking forever.
func (m *Manager) stop() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopCh == nil {
		m.stopCh = make(chan struct{})
	}
	return m.stopCh
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
		inst.onSubEvent = m.handleSubEvents
		m.instances[name] = inst
		for _, c := range mf.Provides {
			m.providers[c] = name
		}
		started = append(started, name)
		m.setOwner(inst.session, name, inst.extVer, true, "started")
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
		m.setOwnerLocked(old.session, name, old.extVer, false, "restarted")
		old.shutdown(m.cfg.ShutdownGrace)
		delete(m.instances, name)
	}
	inst, err := spawn(mf, m.cfg.HandshakeTimeout)
	if err != nil {
		return err
	}
	inst.onSubEvent = m.handleSubEvents
	m.instances[name] = inst
	for _, c := range mf.Provides {
		m.providers[c] = name
	}
	m.setOwnerLocked(inst.session, name, inst.extVer, true, "restarted")
	var resume []protocol.SubProgress
	for _, s := range m.subs {
		if s.extension == name && s.active && inst.subs[s.name] {
			resume = append(resume, protocol.SubProgress{Sub: s.name, NextID: s.ackedUpTo + 1})
		}
	}
	m.mu.Unlock()
	// Resynchronise the subscription stream with the fresh process: it
	// restarts its ID counter at 1, so without this it would replay
	// events the host already confirmed.
	if len(resume) > 0 {
		if err := inst.loadSubs(m.nextReqID(), resume, m.cfg.TaskTimeout); err != nil {
			return fmt.Errorf("resume subscriptions after restart of %s: %w", name, err)
		}
	}
	m.mu.Lock()
	return nil
}

// stagedItem is one extension in an in-progress upgrade.
type stagedItem struct {
	name string
	new  *Instance // nil until spawned
	old  *Instance // nil when there was no running incarnation
}

// stagedUpgrade carries a prepared upgrade from the preparation phase
// to the confirmation/cutover phase.
type stagedUpgrade struct {
	id       string
	next     map[string]Manifest
	order    []string
	staged   []stagedItem
	byName   map[string]*stagedItem
	prepared map[string]*Instance
	stop     <-chan struct{}
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
func (m *Manager) Upgrade(candidates []Manifest) error {
	m.upgradeMu.Lock()
	m.upgrading.Add(1)
	defer m.upgrading.Done()
	defer m.upgradeMu.Unlock()
	su, err := m.prepareUpgrade(candidates)
	if err != nil {
		return err
	}
	return m.finishUpgrade(su, true)
}

// prepareUpgrade validates the candidate set, then spawns and verifies
// every candidate while the old combination keeps serving. The caller
// must hold upgradeMu. Any failure tears down only the candidates.
func (m *Manager) prepareUpgrade(candidates []Manifest) (*stagedUpgrade, error) {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil, fmt.Errorf("manager is shutting down")
	}
	m.switchSeq++
	upgradeID := fmt.Sprintf("upgrade-%d", m.switchSeq)
	stop := m.stopLocked()
	m.mu.Unlock()

	// Build the post-upgrade manifest set: candidates replace existing
	// manifests with the same name; other extensions are kept.
	next := map[string]Manifest{}
	for name, mf := range m.manifests {
		next[name] = mf
	}
	for _, mf := range candidates {
		if _, known := next[mf.Name]; !known {
			return nil, fmt.Errorf("upgrade unknown extension %q", mf.Name)
		}
		next[mf.Name] = mf
	}
	order, err := resolveOrder(next)
	if err != nil {
		for _, mf := range candidates {
			m.recordSwitch(SwitchAbort, upgradeID, mf.Name, nil, nil, err.Error())
		}
		return nil, err
	}

	// Snapshot current instances/providers for dependency checks and
	// for restoring ownership if the upgrade must be abandoned.
	m.mu.Lock()
	liveProviders := map[string]string{}
	for c, p := range m.providers {
		liveProviders[c] = p
	}
	items := make([]stagedItem, 0, len(candidates))
	byName := map[string]*stagedItem{}
	for _, mf := range candidates {
		item := &stagedItem{name: mf.Name, old: m.instances[mf.Name]}
		items = append(items, *item)
	}
	// Rebuild pointers into a stable backing slice.
	staged := make([]stagedItem, len(items))
	for i := range items {
		staged[i] = items[i]
		byName[items[i].name] = &staged[i]
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

	prepared := map[string]*Instance{}
	for _, name := range order {
		item := byName[name]
		if item == nil {
			continue
		}
		mf := next[name]
		inst, spawnErr := m.spawnCandidate(mf, stop)
		if spawnErr != nil {
			m.abortUpgrade(upgradeID, staged, prepared, spawnErr, stop)
			return nil, spawnErr
		}
		inst.onSubEvent = m.handleSubEvents
		if err := m.verifyInstance(mf, inst, candidateProviders); err != nil {
			inst.shutdown(0)
			m.abortUpgrade(upgradeID, staged, prepared, err, stop)
			return nil, err
		}
		item.new = inst
		prepared[name] = inst
		old := item.old
		m.recordSwitch(SwitchPrepared, upgradeID, name, old, inst, "handshake and capability checks passed")
	}
	return &stagedUpgrade{
		id:       upgradeID,
		next:     next,
		order:    order,
		staged:   staged,
		byName:   byName,
		prepared: prepared,
		stop:     stop,
	}, nil
}

// finishUpgrade cuts traffic over to the prepared candidates, drains
// the old incarnations in reverse dependency order, and only then
// records activation. The caller must hold upgradeMu.
//
// When autoSubs is set (the non-interactive Upgrade path), every
// active subscription of an affected extension is handed to the
// candidate automatically — but only when the handoff is provably
// safe: no unconfirmed events, no emitted-but-unseen gaps, and the
// candidate declared the subscription. Anything uncertain aborts the
// upgrade and points the user at the interactive PrepareUpgrade flow;
// the automatic path never silently decides over uncertain progress.
func (m *Manager) finishUpgrade(su *stagedUpgrade, autoSubs bool) error {
	upgradeID := su.id
	order := su.order
	staged := su.staged
	byName := su.byName
	next := su.next
	stop := su.stop
	// All candidates are valid. Cut over atomically: new tasks now go
	// only to the new instances. Calls already in flight keep holding
	// the old Instance pointer, so a task is never delivered twice.
	if autoSubs {
		for i := range staged {
			item := &staged[i]
			if item.old == nil || item.new == nil {
				continue
			}
			progress, err := item.old.querySubs(m.nextReqID(), m.cfg.TaskTimeout)
			if err != nil {
				m.abortUpgrade(upgradeID, staged, su.prepared, err, stop)
				return fmt.Errorf("read %s subscription progress: %w", item.name, err)
			}
			if problems := m.subUncertainty(item, progress); len(problems) > 0 {
				err := fmt.Errorf("upgrade of %s needs an explicit subscription decision (%s); use PrepareUpgrade/ConfirmUpgrade",
					item.name, strings.Join(problems, "; "))
				m.recordSwitch(SwitchAbort, upgradeID, item.name, item.old, item.new, err.Error())
				m.abortUpgrade(upgradeID, staged, su.prepared, err, stop)
				return err
			}
		}
	}
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		closeErr := fmt.Errorf("host closed before cutover")
		m.abortUpgrade(upgradeID, staged, su.prepared, closeErr, stop)
		return closeErr
	}
	for i := range staged {
		item := &staged[i]
		if item.old != nil {
			m.retired = append(m.retired, item.old)
			m.setOwnerLocked(item.old.session, item.name, item.old.extVer, false, "superseded by "+upgradeID)
		}
		m.instances[item.name] = item.new
		m.manifests[item.name] = next[item.name]
		m.setOwnerLocked(item.new.session, item.name, item.new.extVer, true, "activated by "+upgradeID)
		m.recordSwitchLocked(SwitchCutover, upgradeID, item.name, item.old, item.new, "new tasks route to new version")
	}
	m.order = order
	m.mu.Unlock()

	// Drain old incarnations in reverse dependency order so providers
	// outlive their dependents during the handoff. The drain ends for
	// an explicit reason: clean bye/exit, the agreed grace boundary, or
	// host shutdown.
	for i := len(order) - 1; i >= 0; i-- {
		item := byName[order[i]]
		if item == nil || item.old == nil {
			continue
		}
		old := item.old
		reason := old.drain(m.cfg.ShutdownGrace, stop)
		switchDetail := string(reason)
		event := SwitchDrained
		switch {
		case reason == EndGraceKill:
			event = SwitchDrainKilled
			switchDetail = "drain boundary exceeded; old process killed"
		case reason == EndHostClose:
			event = SwitchHostClose
			switchDetail = "host closed; old process killed"
		}
		m.recordSwitch(event, upgradeID, item.name, old, item.new, switchDetail)
		m.mu.Lock()
		m.retired = removeInstance(m.retired, old)
		m.mu.Unlock()
	}
	for i := len(staged) - 1; i >= 0; i-- {
		item := &staged[i]
		m.recordSwitch(SwitchActivated, upgradeID, item.name, item.old, item.new, "handoff complete")
	}
	if autoSubs {
		// Fully confirmed and resumable subscriptions resume on the new
		// build right after the last confirmed event.
		for i := range staged {
			item := &staged[i]
			if item.old == nil || item.new == nil {
				continue
			}
			handoff := m.planSubHandoff(item, nil)
			if err := m.applySubHandoff(item, handoff); err != nil {
				return err
			}
		}
	}
	return nil
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
// and records that the old version was retained. It never touches the
// running instances, so task ownership stays with the old build.
func (m *Manager) abortUpgrade(upgradeID string, staged []stagedItem, prepared map[string]*Instance, cause error, stop <-chan struct{}) {
	for i := len(staged) - 1; i >= 0; i-- {
		item := &staged[i]
		new := item.new
		if new == nil {
			m.recordSwitch(SwitchAbort, upgradeID, item.name, item.old, nil, cause.Error())
			continue
		}
		m.setOwner(new.session, item.name, new.extVer, false, "candidate aborted: "+cause.Error())
		reason := new.drain(m.cfg.ShutdownGrace, stop)
		m.recordSwitch(SwitchRollback, upgradeID, item.name, item.old, new,
			fmt.Sprintf("candidate rejected (%v); removed (%s); old version retained", cause, reason))
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

func (m *Manager) recordSwitchLocked(event, upgradeID, name string, old, new *Instance, detail string) {
	m.recordSwitch(event, upgradeID, name, old, new, detail)
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
	if m.stopCh != nil {
		close(m.stopCh)
	}
	plan := m.plan
	m.plan = nil
	m.mu.Unlock()

	// A prepared-but-unconfirmed upgrade is stopped in the middle:
	// only the candidates are torn down, the old combination stays
	// responsible for its sessions until it drains below.
	if plan != nil {
		m.recordSwitch(SwitchAbort, plan.ID, "", nil, nil, "host closed with a prepared upgrade awaiting confirmation")
		m.abortUpgrade(plan.su.id, plan.su.staged, plan.su.prepared, fmt.Errorf("host closed before confirmation"), plan.su.stop)
	}

	for i := len(m.order) - 1; i >= 0; i-- {
		name := m.order[i]
		m.mu.Lock()
		inst := m.instances[name]
		m.mu.Unlock()
		if inst != nil {
			m.setOwner(inst.session, name, inst.extVer, false, "host shutdown")
			inst.shutdown(m.cfg.ShutdownGrace)
		}
	}
}

func (m *Manager) record(e JournalEntry) {
	if m.cfg.Journal != nil {
		m.cfg.Journal.Record(e)
	}
}
