package host

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"sync"

	"extsys/internal/protocol"
)

type persistedState struct {
	Subscriptions map[string]protocol.SubscriptionScope
	Events        map[string]protocol.EventProgress
}

type decisionRecord struct {
	PlanID       string      `json:"plan_id"`
	Extension    string      `json:"extension"`
	Subscription string      `json:"subscription,omitempty"`
	Decision     Disposition `json:"decision"`
}

type stateEnvelope struct {
	Kind           string                      `json:"kind"`
	Extension      string                      `json:"extension,omitempty"`
	Session        string                      `json:"session,omitempty"`
	Owner          *SessionOwnership           `json:"owner,omitempty"`
	Subscription   *protocol.SubscriptionScope `json:"subscription,omitempty"`
	Removed        bool                        `json:"removed,omitempty"`
	Event          *protocol.EventProgress     `json:"event,omitempty"`
	PlanID         string                      `json:"plan_id,omitempty"`
	SubscriptionID string                      `json:"subscription_id,omitempty"`
	Decision       Disposition                 `json:"decision,omitempty"`
	Manifest       *Manifest                   `json:"manifest,omitempty"`
}

// StateStore is an append-only durable log of host-owned subscription,
// event, ownership, and switch-decision progress. A torn final line is
// ignored during replay; every preceding complete line remains usable.
type StateStore struct {
	mu sync.Mutex
	f  *os.File
	w  *bufio.Writer

	state     map[string]*persistedState
	owners    map[string]SessionOwnership
	manifests map[string]Manifest
	decisions []decisionRecord
}

func OpenStateStore(path string) (*StateStore, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s := &StateStore{
		f:         f,
		w:         bufio.NewWriter(f),
		state:     map[string]*persistedState{},
		owners:    map[string]SessionOwnership{},
		manifests: map[string]Manifest{},
	}
	if err := s.replay(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

func (s *StateStore) replay() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	scanner := bufio.NewScanner(s.f)
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec stateEnvelope
		if err := json.Unmarshal(line, &rec); err != nil {
			continue
		}
		s.applyLocked(rec)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

func (s *StateStore) extensionState(extension string) *persistedState {
	st := s.state[extension]
	if st == nil {
		st = &persistedState{
			Subscriptions: map[string]protocol.SubscriptionScope{},
			Events:        map[string]protocol.EventProgress{},
		}
		s.state[extension] = st
	}
	return st
}

func (s *StateStore) applyLocked(rec stateEnvelope) {
	switch rec.Kind {
	case "owner":
		if rec.Session != "" && rec.Owner != nil {
			owner := *rec.Owner
			owner.Active = false
			s.owners[rec.Session] = owner
		}
	case "subscription":
		if rec.Extension == "" || rec.Subscription == nil {
			return
		}
		st := s.extensionState(rec.Extension)
		if rec.Removed {
			delete(st.Subscriptions, rec.Subscription.ID)
		} else {
			st.Subscriptions[rec.Subscription.ID] = *rec.Subscription
		}
	case "event":
		if rec.Extension == "" || rec.Event == nil {
			return
		}
		st := s.extensionState(rec.Extension)
		ev := *rec.Event
		k := eventKey(ev)
		cur := st.Events[k]
		if eventRank(ev.Status) > eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position > cur.Position) {
			st.Events[k] = ev
		}
	case "decision":
		s.decisions = append(s.decisions, decisionRecord{
			PlanID:       rec.PlanID,
			Extension:    rec.Extension,
			Subscription: rec.SubscriptionID,
			Decision:     rec.Decision,
		})
	case "manifest":
		if rec.Manifest != nil && rec.Manifest.Name != "" {
			s.manifests[rec.Manifest.Name] = *rec.Manifest
		}
	}
}

func (s *StateStore) append(rec stateEnvelope) error {
	b, err := json.Marshal(&rec)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(append(b, '\n')); err != nil {
		return err
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *StateStore) SaveOwnerSession(session string, owner SessionOwnership) error {
	if s == nil || session == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	s.owners[session] = owner
	return s.append(stateEnvelope{Kind: "owner", Session: session, Owner: &owner})
}

func (s *StateStore) SaveSubscriptions(extension string, subs []protocol.SubscriptionScope) error {
	if s == nil || extension == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	st := s.extensionState(extension)
	for _, sub := range subs {
		st.Subscriptions[sub.ID] = sub
		if err := s.append(stateEnvelope{Kind: "subscription", Extension: extension, Subscription: &sub}); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateStore) RemoveSubscription(extension, id string) error {
	if s == nil || extension == "" || id == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	st := s.extensionState(extension)
	delete(st.Subscriptions, id)
	sub := protocol.SubscriptionScope{ID: id}
	return s.append(stateEnvelope{Kind: "subscription", Extension: extension, Subscription: &sub, Removed: true})
}

func (s *StateStore) SaveEvents(extension string, events []protocol.EventProgress) error {
	if s == nil || extension == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	st := s.extensionState(extension)
	for _, ev := range events {
		k := eventKey(ev)
		cur := st.Events[k]
		if eventRank(ev.Status) < eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position <= cur.Position) {
			continue
		}
		st.Events[k] = ev
		copy := ev
		if err := s.append(stateEnvelope{Kind: "event", Extension: extension, Event: &copy}); err != nil {
			return err
		}
	}
	return nil
}

func (s *StateStore) SaveDecision(planID, extension, subscriptionID string, decision Disposition) error {
	if s == nil || planID == "" || extension == "" || decision == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.decisions = append(s.decisions, decisionRecord{PlanID: planID, Extension: extension, Subscription: subscriptionID, Decision: decision})
	return s.append(stateEnvelope{Kind: "decision", PlanID: planID, Extension: extension, SubscriptionID: subscriptionID, Decision: decision})
}

func (s *StateStore) SaveManifest(mf Manifest) error {
	if s == nil || mf.Name == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.manifests[mf.Name] = mf
	return s.append(stateEnvelope{Kind: "manifest", Manifest: &mf})
}

func (s *StateStore) Manifests() map[string]Manifest {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]Manifest, len(s.manifests))
	for name, mf := range s.manifests {
		out[name] = mf
	}
	return out
}

func (s *StateStore) Restore(extension string) ([]protocol.SubscriptionScope, []protocol.EventProgress) {
	if s == nil {
		return nil, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.state[extension]
	if st == nil {
		return nil, nil
	}
	subs := make([]protocol.SubscriptionScope, 0, len(st.Subscriptions))
	for _, sub := range st.Subscriptions {
		subs = append(subs, sub)
	}
	events := make([]protocol.EventProgress, 0, len(st.Events))
	for _, ev := range st.Events {
		events = append(events, ev)
	}
	return subs, events
}

func (s *StateStore) Owners() map[string]SessionOwnership {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]SessionOwnership, len(s.owners))
	for session, owner := range s.owners {
		out[session] = owner
	}
	return out
}

func (s *StateStore) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		_ = s.f.Close()
		s.f = nil
		return err
	}
	err := s.f.Close()
	s.f = nil
	return err
}
