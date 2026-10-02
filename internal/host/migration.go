package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SessionDecision tells Commit how to handle one session whose state
// contains items the candidate build cannot interpret.
type SessionDecision string

const (
	// DecisionMigrate applies the accepted items and drops the
	// rejected ones. It is only valid as an explicit user choice for
	// sessions with incompatible items.
	DecisionMigrate SessionDecision = "migrate"
	// DecisionCancel aborts the whole upgrade; the old version keeps
	// serving and retains all session state.
	DecisionCancel SessionDecision = "cancel"
)

// StateItemView is one state item as shown in the upgrade preview.
type StateItemView struct {
	Key      string          `json:"key"`
	Value    json.RawMessage `json:"value,omitempty"`
	Accepted bool            `json:"accepted"`
	Reason   string          `json:"reason,omitempty"`
}

// SessionCheck describes how one running session's state fares against
// the candidate build: which items it accepts and which it cannot
// interpret.
type SessionCheck struct {
	Extension  string          `json:"extension"`
	Session    string          `json:"session"`
	OldVersion string          `json:"old_version"`
	NewVersion string          `json:"new_version"`
	NewSession string          `json:"new_session"`
	Items      []StateItemView `json:"items"`
}

// HasRejected reports whether the candidate cannot interpret at least
// one state item of this session.
func (c *SessionCheck) HasRejected() bool {
	for _, it := range c.Items {
		if !it.Accepted {
			return true
		}
	}
	return false
}

// SessionOwnership is one ledger entry recording which version is (or
// was) responsible for a session. The full history stays queryable
// across consecutive upgrades, failed candidates and aborted
// preparations.
type SessionOwnership struct {
	Time      time.Time `json:"time"`
	Extension string    `json:"extension"`
	Session   string    `json:"session"`
	Version   string    `json:"version"`
	Status    string    `json:"status"` // active | retired | retained
	Detail    string    `json:"detail,omitempty"`
}

// UpgradePlan is a prepared, not yet activated upgrade. The candidate
// processes are running and verified, the old sessions' state has been
// checked against them, and the old versions keep serving all traffic
// until Commit. The user inspects Checks, then either commits with an
// explicit disposition per affected session or cancels.
type UpgradePlan struct {
	ID string

	m        *Manager
	stop     <-chan struct{}
	staged   []stagedItem
	prepared map[string]*Instance
	order    []string
	next     map[string]Manifest
	checks   []SessionCheck

	mu   sync.Mutex
	done bool
}

// Checks returns the per-session state compatibility preview: every
// state item of every running session, marked accepted or rejected by
// the candidate build.
func (p *UpgradePlan) Checks() []SessionCheck {
	out := make([]SessionCheck, len(p.checks))
	copy(out, p.checks)
	return out
}

// Commit activates the prepared upgrade. For every session with
// incompatible state items the caller must pass an explicit decision
// keyed by session token: DecisionMigrate drops the rejected items,
// DecisionCancel aborts the whole upgrade. State changes the old
// session produced while the plan was being prepared are re-snapshotted
// and carried over, so confirmed changes are never lost; once the new
// version has accepted the state, late results from the old session
// cannot overwrite it.
func (p *UpgradePlan) Commit(decisions map[string]SessionDecision) error {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return fmt.Errorf("upgrade plan %s already resolved", p.ID)
	}
	p.mu.Unlock()

	m := p.m
	for _, c := range p.checks {
		d := decisions[c.Session]
		if d == DecisionCancel {
			reason := fmt.Sprintf("user cancelled during state review (session %s of %s)", c.Session, c.Extension)
			_ = p.Cancel(reason)
			return fmt.Errorf("upgrade %s cancelled: %s", p.ID, reason)
		}
		if c.HasRejected() && d != DecisionMigrate {
			return fmt.Errorf("session %s of %s has state items the candidate cannot interpret; "+
				"an explicit decision (migrate or cancel) is required", c.Session, c.Extension)
		}
	}

	p.mu.Lock()
	p.done = true
	p.mu.Unlock()
	defer p.finish()

	m.recordSwitch(SwitchConfirmed, p.ID, "", nil, nil, "user confirmed the state disposition")

	// Barrier: stop new dispatches from slipping between the final
	// snapshot and the cutover, and wait for work the old session
	// already accepted to finish so its state changes are captured.
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		cause := errors.New("host closed before cutover")
		m.abortUpgrade(p.ID, p.staged, p.prepared, cause, p.stop)
		return cause
	}
	for i := range p.staged {
		item := &p.staged[i]
		if item.old == nil {
			continue
		}
		if err := waitDrained(item.old, m.cfg.TaskTimeout+m.cfg.ShutdownGrace, p.stop); err != nil {
			m.mu.Unlock()
			m.abortUpgrade(p.ID, p.staged, p.prepared, err, p.stop)
			return err
		}
	}

	// Re-snapshot every old session: preparation may have taken a
	// while and the confirmed state changes since the check must not
	// be lost. Items that changed (or appeared) since the preview are
	// re-checked against the candidate.
	for ci := range p.checks {
		c := &p.checks[ci]
		item := p.itemFor(c.Extension)
		if item == nil || item.old == nil {
			continue
		}
		latest, err := item.old.snapshotState("state-final-"+p.ID+"-"+c.Extension, m.cfg.TaskTimeout)
		if err != nil {
			m.mu.Unlock()
			m.abortUpgrade(p.ID, p.staged, p.prepared, err, p.stop)
			return fmt.Errorf("final state snapshot of %s: %w", c.Extension, err)
		}
		changed := changedItems(latest, c.Items)
		verdicts := map[string]StateItemView{}
		for _, it := range c.Items {
			verdicts[it.Key] = it
		}
		if len(changed) > 0 {
			verdict, err := item.new.checkState("state-recheck-"+p.ID+"-"+c.Extension, changed, m.cfg.TaskTimeout)
			if err != nil {
				m.mu.Unlock()
				m.abortUpgrade(p.ID, p.staged, p.prepared, err, p.stop)
				return fmt.Errorf("state re-check of %s: %w", c.Extension, err)
			}
			for k, v := range verdict.Accepted {
				verdicts[k] = StateItemView{Key: k, Value: v, Accepted: true}
			}
			for k, reason := range verdict.Rejected {
				verdicts[k] = StateItemView{Key: k, Value: latest[k], Accepted: false, Reason: reason}
			}
		}
		accepted := map[string]json.RawMessage{}
		var newRejected []string
		for k, v := range latest {
			it, known := verdicts[k]
			switch {
			case !known:
				// Never silently drop state: refuse the commit.
				newRejected = append(newRejected, k)
			case it.Accepted:
				accepted[k] = v
			case decisions[c.Session] != DecisionMigrate:
				newRejected = append(newRejected, k)
			}
		}
		if len(newRejected) > 0 {
			m.mu.Unlock()
			cause := fmt.Errorf("session %s of %s gained incompatible state items during preparation: %v",
				c.Session, c.Extension, newRejected)
			m.abortUpgrade(p.ID, p.staged, p.prepared, cause, p.stop)
			return cause
		}
		if err := item.new.applyState("state-apply-"+p.ID+"-"+c.Extension, accepted, m.cfg.TaskTimeout); err != nil {
			m.mu.Unlock()
			m.abortUpgrade(p.ID, p.staged, p.prepared, err, p.stop)
			return fmt.Errorf("apply state to new %s: %w", c.Extension, err)
		}
	}

	// All state is accepted by the new versions. Cut over atomically.
	for i := range p.staged {
		item := &p.staged[i]
		if item.old != nil {
			item.old.markRetired()
			m.retired = append(m.retired, item.old)
			m.noteOwnershipLocked(item.name, item.old, "retired",
				"superseded by "+item.new.extVer+" ("+item.new.session+")")
		}
		m.instances[item.name] = item.new
		m.manifests[item.name] = p.next[item.name]
		m.noteOwnershipLocked(item.name, item.new, "active", "cutover of "+p.ID)
		m.recordSwitchLocked(SwitchCutover, p.ID, item.name, item.old, item.new, "new tasks route to new version")
	}
	m.order = p.order
	m.mu.Unlock()

	m.drainAndActivate(p.ID, p.staged, p.order)
	return nil
}

// Cancel tears down every prepared candidate and keeps the old
// versions serving. The audit trail records the abort and that the old
// sessions were retained.
func (p *UpgradePlan) Cancel(reason string) error {
	p.mu.Lock()
	if p.done {
		p.mu.Unlock()
		return fmt.Errorf("upgrade plan %s already resolved", p.ID)
	}
	p.done = true
	p.mu.Unlock()
	defer p.finish()
	if reason == "" {
		reason = "cancelled by user"
	}
	p.m.abortUpgrade(p.ID, p.staged, p.prepared, errors.New(reason), p.stop)
	return nil
}

// finish releases the upgrade serialisation so the next upgrade or
// plan can proceed.
func (p *UpgradePlan) finish() {
	m := p.m
	m.mu.Lock()
	delete(m.pendingPlans, p)
	m.mu.Unlock()
	m.upgrading.Done()
	m.upgradeMu.Unlock()
}

func (p *UpgradePlan) itemFor(name string) *stagedItem {
	for i := range p.staged {
		if p.staged[i].name == name {
			return &p.staged[i]
		}
	}
	return nil
}

// changedItems returns the items whose value differs from the checked
// snapshot, plus items that appeared since.
func changedItems(latest map[string]json.RawMessage, checked []StateItemView) map[string]json.RawMessage {
	prev := map[string]json.RawMessage{}
	for _, it := range checked {
		prev[it.Key] = it.Value
	}
	changed := map[string]json.RawMessage{}
	for k, v := range latest {
		old, ok := prev[k]
		if !ok || string(old) != string(v) {
			changed[k] = v
		}
	}
	return changed
}

// waitDrained blocks until the instance has no in-flight tasks, the
// timeout elapses, or the host starts shutting down.
func waitDrained(inst *Instance, timeout time.Duration, stop <-chan struct{}) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	deadline := time.After(timeout)
	for inst.inFlight() > 0 {
		select {
		case <-deadline:
			return fmt.Errorf("old %s still has %d in-flight tasks after %s", inst.name, inst.inFlight(), timeout)
		case <-stop:
			return fmt.Errorf("host closed while waiting for old %s to quiesce", inst.name)
		case <-time.After(5 * time.Millisecond):
		}
	}
	return nil
}

// RollbackReport describes a downgrade: which state items the previous
// build accepted back, and whether the session state can be considered
// restored at all.
type RollbackReport struct {
	Extension     string          `json:"extension"`
	FromVersion   string          `json:"from_version"`
	FromSession   string          `json:"from_session"`
	ToVersion     string          `json:"to_version"`
	ToSession     string          `json:"to_session"`
	Items         []StateItemView `json:"items"`
	StateRestored bool            `json:"state_restored"`
	Detail        string          `json:"detail"`
}

// Rollback returns one extension to a previous build. Like an upgrade,
// the current session state is checked against the build being
// restored; the report states exactly which items the old build
// accepted. When it rejected items, StateRestored is false — the old
// process runs again, but the session state is only partially carried
// back and the caller must not treat it as restored.
func (m *Manager) Rollback(name string, previous Manifest) (*RollbackReport, error) {
	plan, err := m.PlanUpgrade([]Manifest{previous})
	if err != nil {
		return nil, err
	}
	checks := plan.Checks()
	decisions := map[string]SessionDecision{}
	for _, c := range checks {
		// A rollback always carries back everything the old build
		// accepts; rejected items are reported, not silently dropped.
		decisions[c.Session] = DecisionMigrate
	}
	if err := plan.Commit(decisions); err != nil {
		return nil, err
	}
	rep := &RollbackReport{Extension: name, StateRestored: true}
	for _, c := range checks {
		if c.Extension != name {
			continue
		}
		rep.FromVersion, rep.FromSession = c.OldVersion, c.Session
		rep.ToVersion, rep.ToSession = c.NewVersion, c.NewSession
		rep.Items = c.Items
		for _, it := range c.Items {
			if !it.Accepted {
				rep.StateRestored = false
			}
		}
	}
	if rep.StateRestored {
		rep.Detail = "old build accepted every state item; session state fully carried back"
	} else {
		rep.Detail = "old build rejected state items; old process restored but session state only partially carried back"
	}
	m.recordSwitch(SwitchRollback, plan.ID, name, nil, nil, rep.Detail)
	return rep, nil
}
