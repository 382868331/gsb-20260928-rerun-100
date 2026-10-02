package host

import (
	"fmt"
	"sort"
	"sync"

	"extsys/internal/protocol"
)

// SessionOwnership records which extension version is responsible for
// one negotiated session. It is updated at every lifecycle transition
// (start, restart, cutover, abort, shutdown), so after consecutive
// upgrades, dying candidates, or a preparation stopped mid-way, the
// final owner of every session can always be queried.
type SessionOwnership struct {
	Extension string
	Version   string
	Active    bool   // true while this incarnation owns new work
	Note      string // why this ownership state was reached
}

// SessionOwner reports who is responsible for the given session token.
func (m *Manager) SessionOwner(session string) (SessionOwnership, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	owner, ok := m.owners[session]
	return owner, ok
}

func (m *Manager) setOwner(session, ext, ver string, active bool, note string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setOwnerLocked(session, ext, ver, active, note)
}

func (m *Manager) setOwnerLocked(session, ext, ver string, active bool, note string) {
	if session == "" {
		return
	}
	m.owners[session] = SessionOwnership{Extension: ext, Version: ver, Active: active, Note: note}
}

// Disposition is the user's decision for one session whose saved state
// contains items the candidate cannot interpret.
type Disposition string

const (
	// DecideMigrate loads every accepted item into the new version and
	// drops the rejected ones.
	DecideMigrate Disposition = "migrate"
	// DecideDiscard drops the session's saved state entirely.
	DecideDiscard Disposition = "discard"
	// DecideCancel aborts the whole upgrade; the old version keeps
	// serving and keeps its state.
	DecideCancel Disposition = "cancel"
)

// SessionStatePreview describes, for one extension, the state the old
// session has saved and how the candidate build reacts to it.
type SessionStatePreview struct {
	Extension  string
	OldVersion string
	OldSession string
	NewVersion string
	NewSession string
	Items      []protocol.StateItem // state saved by the old session
	Accepted   []string             // keys the candidate can interpret
	Rejected   []string             // keys the candidate cannot interpret
}

// UpgradePlan is a prepared upgrade awaiting user confirmation. While
// the plan is open the old version keeps serving (and keeps producing
// state changes); nothing is cut over until ConfirmUpgrade.
type UpgradePlan struct {
	ID       string
	Sessions []SessionStatePreview

	su *stagedUpgrade
}

// PrepareUpgrade spawns and verifies the candidates, then inspects
// every affected session: it dumps the old version's saved state and
// asks each candidate which items it can interpret. The result is a
// preview the user can cancel, or confirm with explicit dispositions
// for sessions that have incompatible items. The plan stays open until
// ConfirmUpgrade or CancelUpgrade; overlapping upgrades are serialised
// until then.
func (m *Manager) PrepareUpgrade(candidates []Manifest) (*UpgradePlan, error) {
	m.upgradeMu.Lock()
	m.upgrading.Add(1)
	su, err := m.prepareUpgrade(candidates)
	if err != nil {
		m.upgrading.Done()
		m.upgradeMu.Unlock()
		return nil, err
	}
	plan := &UpgradePlan{ID: su.id, su: su}
	for i := range su.staged {
		item := &su.staged[i]
		if item.old == nil || item.new == nil {
			continue
		}
		preview := SessionStatePreview{
			Extension:  item.name,
			OldVersion: item.old.extVer,
			OldSession: item.old.session,
			NewVersion: item.new.extVer,
			NewSession: item.new.session,
		}
		items, err := item.old.queryState(m.nextReqID(), m.cfg.TaskTimeout)
		if err != nil {
			m.abortUpgrade(su.id, su.staged, su.prepared, err, su.stop)
			m.upgrading.Done()
			m.upgradeMu.Unlock()
			return nil, fmt.Errorf("dump %s session state: %w", item.name, err)
		}
		accepted, rejected, err := item.new.checkState(m.nextReqID(), items, m.cfg.TaskTimeout)
		if err != nil {
			m.abortUpgrade(su.id, su.staged, su.prepared, err, su.stop)
			m.upgrading.Done()
			m.upgradeMu.Unlock()
			return nil, fmt.Errorf("check %s state compatibility: %w", item.name, err)
		}
		preview.Items = items
		preview.Accepted = accepted
		preview.Rejected = rejected
		sort.Strings(preview.Accepted)
		sort.Strings(preview.Rejected)
		m.recordSwitch(SwitchStateCheck, su.id, item.name, item.old, item.new,
			fmt.Sprintf("old session holds %d state items; candidate accepts %d, rejects %d",
				len(items), len(accepted), len(rejected)))
		plan.Sessions = append(plan.Sessions, preview)
	}
	m.mu.Lock()
	m.plan = plan
	m.mu.Unlock()
	return plan, nil
}

// takePlan removes and returns the pending plan if the id matches.
func (m *Manager) takePlan(id string) *UpgradePlan {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.plan == nil || m.plan.ID != id {
		return nil
	}
	plan := m.plan
	m.plan = nil
	return plan
}

// ConfirmUpgrade applies the user's dispositions and activates the
// prepared upgrade. Sessions with incompatible (rejected) items require
// an explicit decision; a missing decision leaves the plan open so the
// user can still decide. DecideCancel on any session aborts the whole
// upgrade and retains the old version.
//
// State saved by the old session is re-dumped here, so changes produced
// while the plan was open are not lost. After the new version accepts
// the state, late pushes from the old process are only forwarded when
// they carry a strictly newer version, so a stale result can never
// overwrite what the new version already accepted.
func (m *Manager) ConfirmUpgrade(planID string, decisions map[string]Disposition) error {
	m.mu.Lock()
	plan := m.plan
	m.mu.Unlock()
	if plan == nil || plan.ID != planID {
		return fmt.Errorf("no prepared upgrade %q awaiting confirmation", planID)
	}

	// Every session with incompatible items needs an explicit decision.
	// A missing decision leaves the plan (and the upgrade lock) open so
	// the user can still decide; only a closing path releases them.
	for _, s := range plan.Sessions {
		d := decisions[s.Extension]
		if d == DecideCancel {
			m.recordSwitch(SwitchConfirmed, plan.ID, s.Extension, nil, nil, "user cancelled the upgrade")
			m.abortUpgrade(plan.su.id, plan.su.staged, plan.su.prepared,
				fmt.Errorf("cancelled by user"), plan.su.stop)
			m.takePlan(planID)
			m.upgrading.Done()
			m.upgradeMu.Unlock()
			return fmt.Errorf("upgrade %s cancelled by user", planID)
		}
		if len(s.Rejected) > 0 && d != DecideMigrate && d != DecideDiscard {
			return fmt.Errorf("session %s (%s) has %d incompatible state items; explicit decision required",
				s.Extension, s.OldSession, len(s.Rejected))
		}
	}

	plan = m.takePlan(planID)
	if plan == nil {
		return fmt.Errorf("prepared upgrade %q was interrupted", planID)
	}
	defer m.upgrading.Done()
	defer m.upgradeMu.Unlock()

	byName := map[string]SessionStatePreview{}
	for _, s := range plan.Sessions {
		byName[s.Extension] = s
	}
	for i := range plan.su.staged {
		item := &plan.su.staged[i]
		if item.old == nil || item.new == nil {
			continue
		}
		decision := decisions[item.name]
		if decision == "" {
			decision = DecideMigrate // fully compatible: nothing to decide
		}
		accepted := map[string]bool{}
		for _, k := range byName[item.name].Accepted {
			accepted[k] = true
		}
		migrated, err := m.transferState(item, decision, accepted)
		if err != nil {
			m.abortUpgrade(plan.su.id, plan.su.staged, plan.su.prepared, err, plan.su.stop)
			return err
		}
		m.recordSwitch(SwitchConfirmed, plan.ID, item.name, item.old, item.new,
			fmt.Sprintf("user decision %q; %d state items handed to the new version", decision, migrated))
	}
	m.finishUpgrade(plan.su)
	return nil
}

// CancelUpgrade aborts a prepared upgrade. The old version keeps
// serving and keeps its state; only the candidates are torn down.
func (m *Manager) CancelUpgrade(planID string, reason string) error {
	plan := m.takePlan(planID)
	if plan == nil {
		return fmt.Errorf("no prepared upgrade %q awaiting confirmation", planID)
	}
	defer m.upgrading.Done()
	defer m.upgradeMu.Unlock()
	if reason == "" {
		reason = "cancelled by user"
	}
	m.recordSwitch(SwitchConfirmed, plan.ID, "", nil, nil, "user declined the upgrade: "+reason)
	m.abortUpgrade(plan.su.id, plan.su.staged, plan.su.prepared, fmt.Errorf("%s", reason), plan.su.stop)
	return nil
}

// transferState re-dumps the old session's state (merging any pushes
// observed while the plan was open), loads the accepted portion into
// the new instance, and installs a sink so late pushes from the old
// process only ever advance the new one's state. Returns how many
// items were handed over.
func (m *Manager) transferState(item *stagedItem, decision Disposition, accepted map[string]bool) (int, error) {
	old, next := item.old, item.new
	keep := func(key string) bool {
		return decision == DecideMigrate && accepted[key]
	}
	items, err := old.queryState(m.nextReqID(), m.cfg.TaskTimeout)
	if err != nil {
		return 0, fmt.Errorf("re-dump %s session state before cutover: %w", item.name, err)
	}
	var load []protocol.StateItem
	loaded := map[string]uint64{}
	for _, it := range items {
		if keep(it.Key) {
			load = append(load, it)
			loaded[it.Key] = it.Version
		}
	}
	if err := next.loadState(m.nextReqID(), load, m.cfg.TaskTimeout); err != nil {
		return 0, fmt.Errorf("load state into new %s: %w", item.name, err)
	}
	var mu sync.Mutex
	old.setStateSink(func(fresh []protocol.StateItem) {
		mu.Lock()
		var fwd []protocol.StateItem
		for _, it := range fresh {
			if keep(it.Key) && it.Version > loaded[it.Key] {
				fwd = append(fwd, it)
				loaded[it.Key] = it.Version
			}
		}
		mu.Unlock()
		if len(fwd) > 0 {
			_ = next.loadState(m.nextReqID(), fwd, m.cfg.TaskTimeout)
		}
	})
	return len(load), nil
}

// RollbackReport describes how far the new session state can be
// received by the old build during a rollback. StateFullyAccepted is
// false when the old build cannot interpret some items; in that case
// the process was rolled back but the session state was only partially
// restored, and the rejected items are listed explicitly.
type RollbackReport struct {
	Extension          string
	FromVersion        string // version being rolled back
	FromSession        string
	ToVersion          string // old build taking over again
	ToSession          string
	Accepted           []string
	Rejected           []string
	StateFullyAccepted bool
}

// Rollback replaces the current build of one extension with a previous
// one. Unlike a plain restart it first checks whether the old build can
// receive the state the new version accumulated: accepted items are
// handed over, rejected items are reported, and the caller is told
// whether the session state survived the rollback intact.
func (m *Manager) Rollback(name string, previous Manifest) (*RollbackReport, error) {
	m.upgradeMu.Lock()
	m.upgrading.Add(1)
	defer m.upgrading.Done()
	defer m.upgradeMu.Unlock()

	su, err := m.prepareUpgrade([]Manifest{previous})
	if err != nil {
		return nil, err
	}
	item := &su.staged[0]
	report := &RollbackReport{Extension: name, StateFullyAccepted: true}
	if item.old != nil {
		report.FromVersion, report.FromSession = item.old.extVer, item.old.session
	}
	if item.new != nil {
		report.ToVersion, report.ToSession = item.new.extVer, item.new.session
	}
	if item.old != nil && item.new != nil {
		items, err := item.old.queryState(m.nextReqID(), m.cfg.TaskTimeout)
		if err != nil {
			m.abortUpgrade(su.id, su.staged, su.prepared, err, su.stop)
			return nil, fmt.Errorf("rollback: dump %s session state: %w", name, err)
		}
		accepted, rejected, err := item.new.checkState(m.nextReqID(), items, m.cfg.TaskTimeout)
		if err != nil {
			m.abortUpgrade(su.id, su.staged, su.prepared, err, su.stop)
			return nil, fmt.Errorf("rollback: check %s state compatibility: %w", name, err)
		}
		sort.Strings(accepted)
		sort.Strings(rejected)
		report.Accepted = accepted
		report.Rejected = rejected
		report.StateFullyAccepted = len(rejected) == 0
		m.recordSwitch(SwitchStateCheck, su.id, name, item.old, item.new,
			fmt.Sprintf("rollback: old build accepts %d of %d new-state items, rejects %d",
				len(accepted), len(items), len(rejected)))
		keep := map[string]bool{}
		for _, k := range accepted {
			keep[k] = true
		}
		if _, err := m.transferState(item, DecideMigrate, keep); err != nil {
			m.abortUpgrade(su.id, su.staged, su.prepared, err, su.stop)
			return nil, err
		}
		m.recordSwitch(SwitchConfirmed, su.id, name, item.old, item.new,
			fmt.Sprintf("rollback confirmed; %d state items returned to the old build", len(accepted)))
	}
	m.finishUpgrade(su)
	return report, nil
}

// nextReqID allocates a correlation id for state-migration requests.
func (m *Manager) nextReqID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskSeq++
	return fmt.Sprintf("state-%d", m.taskSeq)
}

// DumpState returns the session state currently saved by a running
// extension, merging its snapshot with any pushes the host has seen.
func (m *Manager) DumpState(name string) ([]protocol.StateItem, error) {
	m.mu.Lock()
	inst := m.instances[name]
	m.mu.Unlock()
	if inst == nil {
		return nil, fmt.Errorf("extension %q is not running", name)
	}
	return inst.queryState(m.nextReqID(), m.cfg.TaskTimeout)
}
