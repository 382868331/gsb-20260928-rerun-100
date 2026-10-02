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
	switchSeq int
	closed    bool
	stopCh    chan struct{}      // closed when host shutdown begins
	retired   []*Instance        // old incarnations draining in flight
	ledger    []SessionOwnership // which version owns/owned each session

	upgradeMu    sync.Mutex // serialises overlapping upgrade attempts
	upgrading    sync.WaitGroup
	pendingPlans map[*UpgradePlan]struct{} // prepared, not yet resolved
}

// New validates manifests and resolves the dependency order. Any
// missing requirement or dependency cycle fails here, before anything
// runs.
func New(manifests []Manifest, cfg Config) (*Manager, error) {
	m := &Manager{
		cfg:          cfg,
		manifests:    map[string]Manifest{},
		instances:    map[string]*Instance{},
		providers:    map[string]string{},
		pendingPlans: map[*UpgradePlan]struct{}{},
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
		m.instances[name] = inst
		for _, c := range mf.Provides {
			m.providers[c] = name
		}
		m.noteOwnershipLocked(name, inst, "active", "initial start")
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
		m.noteOwnershipLocked(name, old, "retired", "replaced by restart")
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
	m.noteOwnershipLocked(name, inst, "active", "restart with fresh session")
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
// PlanUpgrade prepares an upgrade without activating it: candidates
// are spawned and verified, then every running session's state is
// snapshotted and checked against the candidate build. The returned
// plan's Checks describe, per session, which state items the candidate
// accepts and which it cannot interpret; the caller then commits with
// an explicit disposition or cancels. Old versions keep serving all
// traffic until Commit.
func (m *Manager) PlanUpgrade(candidates []Manifest) (*UpgradePlan, error) {
	m.upgradeMu.Lock()
	m.upgrading.Add(1)

	fail := func(err error) (*UpgradePlan, error) {
		m.upgrading.Done()
		m.upgradeMu.Unlock()
		return nil, err
	}

	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return fail(fmt.Errorf("manager is shutting down"))
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
			return fail(fmt.Errorf("upgrade unknown extension %q", mf.Name))
		}
		next[mf.Name] = mf
	}
	order, err := resolveOrder(next)
	if err != nil {
		for _, mf := range candidates {
			m.recordSwitch(SwitchAbort, upgradeID, mf.Name, nil, nil, err.Error())
		}
		return fail(err)
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
			return fail(spawnErr)
		}
		if err := m.verifyInstance(mf, inst, candidateProviders); err != nil {
			inst.shutdown(0)
			m.abortUpgrade(upgradeID, staged, prepared, err, stop)
			return fail(err)
		}
		item.new = inst
		prepared[name] = inst
		old := item.old
		m.recordSwitch(SwitchPrepared, upgradeID, name, old, inst, "handshake and capability checks passed")
	}

	// State check: snapshot every running session and ask the
	// candidate which items it can accept. The old sessions keep
	// producing state changes during this phase; Commit re-snapshots
	// so nothing confirmed in the meantime is lost.
	var checks []SessionCheck
	for i := range staged {
		item := &staged[i]
		if item.old == nil {
			continue
		}
		items, snapErr := item.old.snapshotState("state-check-"+upgradeID+"-"+item.name, m.cfg.TaskTimeout)
		if snapErr != nil {
			m.abortUpgrade(upgradeID, staged, prepared, snapErr, stop)
			return fail(fmt.Errorf("state snapshot of %s: %w", item.name, snapErr))
		}
		verdict, checkErr := item.new.checkState("state-verdict-"+upgradeID+"-"+item.name, items, m.cfg.TaskTimeout)
		if checkErr != nil {
			m.abortUpgrade(upgradeID, staged, prepared, checkErr, stop)
			return fail(fmt.Errorf("state check of %s: %w", item.name, checkErr))
		}
		check := SessionCheck{
			Extension:  item.name,
			Session:    item.old.session,
			OldVersion: item.old.extVer,
			NewVersion: item.new.extVer,
			NewSession: item.new.session,
		}
		for k, v := range verdict.Accepted {
			check.Items = append(check.Items, StateItemView{Key: k, Value: v, Accepted: true})
		}
		for k, reason := range verdict.Rejected {
			check.Items = append(check.Items, StateItemView{Key: k, Value: items[k], Accepted: false, Reason: reason})
		}
		sort.Slice(check.Items, func(a, b int) bool { return check.Items[a].Key < check.Items[b].Key })
		checks = append(checks, check)
		m.recordSwitch(SwitchStateCheck, upgradeID, item.name, item.old, item.new,
			fmt.Sprintf("%d item(s) accepted, %d not interpretable by candidate",
				len(verdict.Accepted), len(verdict.Rejected)))
	}

	plan := &UpgradePlan{
		ID:       upgradeID,
		m:        m,
		stop:     stop,
		staged:   staged,
		prepared: prepared,
		order:    order,
		next:     next,
		checks:   checks,
	}
	m.mu.Lock()
	m.pendingPlans[plan] = struct{}{}
	m.mu.Unlock()
	return plan, nil
}

// Upgrade atomically replaces one or more extension builds. It is the
// automatic form of PlanUpgrade + Commit: it proceeds only when every
// session state item is accepted by the candidate. When any item is
// not interpretable, the upgrade is cancelled and an error names the
// affected sessions — the caller must use PlanUpgrade to review the
// items and choose an explicit disposition.
func (m *Manager) Upgrade(candidates []Manifest) error {
	plan, err := m.PlanUpgrade(candidates)
	if err != nil {
		return err
	}
	decisions := map[string]SessionDecision{}
	for _, c := range plan.Checks() {
		if c.HasRejected() {
			err := fmt.Errorf("session %s of %s has state items the candidate cannot interpret; "+
				"automatic upgrade refused, use PlanUpgrade to choose a disposition", c.Session, c.Extension)
			_ = plan.Cancel(err.Error())
			return err
		}
		decisions[c.Session] = DecisionMigrate
	}
	return plan.Commit(decisions)
}

// drainAndActivate drains old incarnations in reverse dependency order
// so providers outlive their dependents during the handoff, then
// records activation. The drain ends for an explicit reason: clean
// bye/exit, the agreed grace boundary, or host shutdown.
func (m *Manager) drainAndActivate(upgradeID string, staged []stagedItem, order []string) {
	byName := map[string]*stagedItem{}
	for i := range staged {
		byName[staged[i].name] = &staged[i]
	}
	for i := len(order) - 1; i >= 0; i-- {
		item := byName[order[i]]
		if item == nil || item.old == nil {
			continue
		}
		old := item.old
		reason := old.drain(m.cfg.ShutdownGrace, m.stop())
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
			if item.old != nil {
				m.noteOwnership(item.name, item.old, "retained",
					"candidate failed before preparation finished: "+cause.Error())
			}
			m.recordSwitch(SwitchAbort, upgradeID, item.name, item.old, nil, cause.Error())
			continue
		}
		reason := new.drain(m.cfg.ShutdownGrace, stop)
		if item.old != nil {
			m.noteOwnership(item.name, item.old, "retained",
				fmt.Sprintf("candidate rejected (%v) and removed; old version still responsible", cause))
		}
		m.recordSwitch(SwitchRollback, upgradeID, item.name, item.old, new,
			fmt.Sprintf("candidate rejected (%v); removed (%s); old version retained", cause, reason))
	}
}

// noteOwnershipLocked appends a session-ownership ledger entry. The
// caller must hold m.mu.
func (m *Manager) noteOwnershipLocked(name string, inst *Instance, status, detail string) {
	m.ledger = append(m.ledger, SessionOwnership{
		Time:      time.Now().UTC(),
		Extension: name,
		Session:   inst.session,
		Version:   inst.extVer,
		Status:    status,
		Detail:    detail,
	})
}

func (m *Manager) noteOwnership(name string, inst *Instance, status, detail string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.noteOwnershipLocked(name, inst, status, detail)
}

// SessionOwnership returns the ownership history of one extension's
// sessions, oldest first: which version was responsible for each
// session across starts, upgrades, failed candidates and aborts.
func (m *Manager) SessionOwnership(name string) []SessionOwnership {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SessionOwnership
	for _, e := range m.ledger {
		if e.Extension == name {
			out = append(out, e)
		}
	}
	return out
}

// SessionOwner reports which version and session is currently
// responsible for the extension's state.
func (m *Manager) SessionOwner(name string) (SessionOwnership, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := len(m.ledger) - 1; i >= 0; i-- {
		if m.ledger[i].Extension == name && m.ledger[i].Status == "active" {
			return m.ledger[i], true
		}
	}
	return SessionOwnership{}, false
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
	plans := make([]*UpgradePlan, 0, len(m.pendingPlans))
	for p := range m.pendingPlans {
		plans = append(plans, p)
	}
	m.mu.Unlock()

	// A prepared-but-never-committed upgrade must not strand its
	// candidates: cancel it so the audit trail records the abort and
	// the old versions remain the session owners.
	for _, p := range plans {
		_ = p.Cancel("host closed during upgrade preparation")
	}

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
