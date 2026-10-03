package host

import (
	"extsys/internal/protocol"
	"fmt"
	"sort"
	"strings"
)

// subEventRecord is the ledger entry for one identified event. The
// owner is the version/session whose delivery the host is using; it is
// frozen once the event is externally confirmed, so a late duplicate
// from a draining process can never rewrite history.
type subEventRecord struct {
	id      uint64
	acked   bool
	version string
	session string
}

// subscription is the host-side ledger for one persistent subscription
// of one extension. It survives cutovers, restarts and rollbacks: the
// ledger, not any single process, is the authoritative record of which
// events were confirmed and by whom.
type subscription struct {
	extension  string
	name       string
	active     bool // false once the user decided to stop it
	events     map[uint64]*subEventRecord
	ackedUpTo  uint64 // contiguous confirmed prefix
	duplicates int    // late/duplicate deliveries dropped after confirmation
}

func subKey(ext, sub string) string { return ext + "/" + sub }

// handleSubEvents records deliveries from any incarnation of an
// extension. A delivery from the currently responsible instance takes
// ownership of a still-unconfirmed event; deliveries from a draining
// predecessor for an event the host already has are duplicates and
// change nothing. Confirmed events are immutable.
func (m *Manager) handleSubEvents(inst *Instance, events []protocol.SubEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, ev := range events {
		key := subKey(inst.name, ev.Sub)
		sub, ok := m.subs[key]
		if !ok {
			sub = &subscription{extension: inst.name, name: ev.Sub, active: true, events: map[uint64]*subEventRecord{}}
			m.subs[key] = sub
		}
		if rec, ok := sub.events[ev.ID]; ok {
			if rec.acked {
				sub.duplicates++
				continue
			}
			// Still unconfirmed: the currently responsible instance's
			// delivery is the one the host will confirm. A late copy
			// from a retired process does not take the event over.
			if cur := m.instances[inst.name]; cur == inst {
				rec.version = inst.extVer
				rec.session = inst.session
			}
			continue
		}
		sub.events[ev.ID] = &subEventRecord{id: ev.ID, version: inst.extVer, session: inst.session}
	}
}

// AckEvent externally confirms one delivered event. The confirmation is
// attributed to the version/session that owns the delivery in the
// ledger; it can never be reassigned afterwards.
func (m *Manager) AckEvent(ext, sub string, id uint64) error {
	m.mu.Lock()
	s, ok := m.subs[subKey(ext, sub)]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("no subscription %q on extension %q", sub, ext)
	}
	rec, ok := s.events[id]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription %q has no event %d", subKey(ext, sub), id)
	}
	if rec.acked {
		m.mu.Unlock()
		return fmt.Errorf("event %s/%d already confirmed", subKey(ext, sub), id)
	}
	rec.acked = true
	for {
		next, ok := s.events[s.ackedUpTo+1]
		if !ok || !next.acked {
			break
		}
		s.ackedUpTo++
	}
	owner := *rec
	m.mu.Unlock()
	if m.cfg.Journal != nil {
		m.cfg.Journal.Ack(AckRecord{
			Extension: ext, Subscription: sub, EventID: id,
			Version: owner.version, Session: owner.session,
		})
	}
	return nil
}

// EventProgress is the externally visible state of one ledger event.
type EventProgress struct {
	ID      uint64
	Acked   bool
	Version string // version responsible for the delivery
	Session string
}

// SubscriptionStatus is a snapshot of one subscription ledger.
type SubscriptionStatus struct {
	Extension  string
	Name       string
	Active     bool
	AckedUpTo  uint64
	Duplicates int
	Events     []EventProgress // sorted by ID
}

// Subscriptions returns the ledger snapshot for one extension.
func (m *Manager) Subscriptions(ext string) []SubscriptionStatus {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SubscriptionStatus
	for _, s := range m.subs {
		if s.extension != ext {
			continue
		}
		out = append(out, s.snapshot())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *subscription) snapshot() SubscriptionStatus {
	st := SubscriptionStatus{
		Extension: s.extension, Name: s.name, Active: s.active,
		AckedUpTo: s.ackedUpTo, Duplicates: s.duplicates,
	}
	for _, rec := range s.events {
		st.Events = append(st.Events, EventProgress{
			ID: rec.id, Acked: rec.acked, Version: rec.version, Session: rec.session,
		})
	}
	sort.Slice(st.Events, func(i, j int) bool { return st.Events[i].ID < st.Events[j].ID })
	return st
}

// SubscriptionPreview describes, for one subscription of an extension
// being upgraded, what the host ledger knows and whether the candidate
// build can take the stream over.
type SubscriptionPreview struct {
	Name      string
	NextID    uint64   // emit position reported by the outgoing build
	AckedUpTo uint64   // last externally confirmed event (contiguous)
	Pending   []uint64 // delivered but not yet confirmed
	Uncertain []uint64 // emitted per the outgoing build but never seen by the host
	CanResume bool     // candidate declared this subscription
}

// NeedsDecision reports whether the subscription has progress the host
// cannot safely migrate on its own: events whose handling cannot be
// determined (emitted but never seen, or last owned by a session that
// is no longer alive to be asked), or a candidate that cannot resume
// the stream at all. Merely unconfirmed events owned by the live
// session are safe: the handoff re-emits them under the new version.
func (p SubscriptionPreview) NeedsDecision() bool {
	return len(p.Uncertain) > 0 || !p.CanResume
}

// subPreviewsLocked builds previews for one staged item. reported is the
// outgoing instance's emit positions (may be nil when unreachable).
// Must be called without holding m.mu; it takes the lock itself.
func (m *Manager) subPreviews(item *stagedItem, reported []protocol.SubProgress) []SubscriptionPreview {
	nextIDs := map[string]uint64{}
	for _, p := range reported {
		nextIDs[p.Sub] = p.NextID
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []SubscriptionPreview
	for _, s := range m.subs {
		if s.extension != item.name || !s.active {
			continue
		}
		p := SubscriptionPreview{
			Name:      s.name,
			NextID:    nextIDs[s.name],
			AckedUpTo: s.ackedUpTo,
			CanResume: item.new != nil && item.new.subs[s.name],
		}
		active := m.instances[item.name]
		for id, rec := range s.events {
			if rec.acked {
				continue
			}
			p.Pending = append(p.Pending, id)
			// An unconfirmed event last owned by a session that is no
			// longer running cannot be asked whether more of the stream
			// is still coming; its progress is not safely determinable.
			if active == nil || active.session != rec.session {
				p.Uncertain = append(p.Uncertain, id)
			}
		}
		// Anything the outgoing build emitted but the host never saw
		// cannot be classified; the user must decide about this range.
		for id := uint64(1); id < p.NextID; id++ {
			if _, ok := s.events[id]; !ok {
				p.Uncertain = append(p.Uncertain, id)
			}
		}
		sort.Slice(p.Pending, func(i, j int) bool { return p.Pending[i] < p.Pending[j] })
		sort.Slice(p.Uncertain, func(i, j int) bool { return p.Uncertain[i] < p.Uncertain[j] })
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// subHandoff is the concrete plan for one subscription at cutover:
// either resume it on the new instance right after the last confirmed
// event, or stop it on the outgoing instance.
type subHandoff struct {
	resume []protocol.SubProgress
	stops  []string
}

// planSubHandoff computes, per active subscription of one staged item,
// whether the new instance resumes it (and from where) or the outgoing
// instance stops it. decisions may force a stop; without a decision the
// subscription is resumed only when the candidate declared it. The
// resume cursor is the last confirmed event, never the last delivered
// one: unconfirmed events are re-emitted by the new build and confirmed
// under its ownership instead of being copied over as if processed.
func (m *Manager) planSubHandoff(item *stagedItem, decisions map[string]Disposition) subHandoff {
	m.mu.Lock()
	defer m.mu.Unlock()
	var h subHandoff
	for _, s := range m.subs {
		if s.extension != item.name || !s.active {
			continue
		}
		if decisions[subKey(item.name, s.name)] == DecideStopSub {
			h.stops = append(h.stops, s.name)
			continue
		}
		if item.new != nil && item.new.subs[s.name] {
			h.resume = append(h.resume, protocol.SubProgress{Sub: s.name, NextID: s.ackedUpTo + 1})
		} else {
			h.stops = append(h.stops, s.name)
		}
	}
	sort.Slice(h.resume, func(i, j int) bool { return h.resume[i].Sub < h.resume[j].Sub })
	sort.Strings(h.stops)
	return h
}

// applySubHandoff stops the subscriptions the user (or the missing
// resume capability) retired on the outgoing instance, marks them
// inactive in the ledger, and loads the resume positions into the new
// instance.
func (m *Manager) applySubHandoff(item *stagedItem, h subHandoff) error {
	for _, name := range h.stops {
		if item.old != nil && item.old.isAlive() {
			if err := item.old.stopSub(m.nextReqID(), name, m.cfg.TaskTimeout); err != nil {
				return fmt.Errorf("stop subscription %s on old %s: %w", subKey(item.name, name), item.name, err)
			}
		}
		// The candidate must not start the stopped stream either.
		if item.new != nil && item.new.isAlive() {
			if err := item.new.stopSub(m.nextReqID(), name, m.cfg.TaskTimeout); err != nil {
				return fmt.Errorf("stop subscription %s on new %s: %w", subKey(item.name, name), item.name, err)
			}
		}
		m.mu.Lock()
		if s, ok := m.subs[subKey(item.name, name)]; ok {
			s.active = false
		}
		m.mu.Unlock()
	}
	if len(h.resume) > 0 && item.new != nil {
		if err := item.new.loadSubs(m.nextReqID(), h.resume, m.cfg.TaskTimeout); err != nil {
			return fmt.Errorf("resume subscriptions on new %s: %w", item.name, err)
		}
	}
	return nil
}

// subUncertaintyLocked describes why the plain (non-interactive)
// upgrade path cannot hand the subscriptions of this extension over
// without a user decision. Empty means an automatic handoff is safe.
func (m *Manager) subUncertainty(item *stagedItem, reported []protocol.SubProgress) []string {
	var problems []string
	for _, p := range m.subPreviews(item, reported) {
		if len(p.Uncertain) > 0 {
			problems = append(problems, fmt.Sprintf("%s: %d emitted events never seen", p.Name, len(p.Uncertain)))
		}
		if !p.CanResume {
			problems = append(problems, p.Name+": candidate cannot resume the subscription")
		}
	}
	return problems
}

// formatIDs compacts an ID list for audit details.
func formatIDs(ids []uint64) string {
	parts := make([]string, len(ids))
	for i, id := range ids {
		parts[i] = fmt.Sprint(id)
	}
	return strings.Join(parts, ",")
}
