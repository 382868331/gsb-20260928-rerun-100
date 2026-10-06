package host

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"extsys/internal/protocol"
)

// Recovery record kinds persisted to the host recovery log. The log is
// an append-only JSON-lines file: every subscription scope, event
// progress update, session-ownership transition, switch decision and
// confirmation watermark is recorded the moment the host observes it,
// so a full host restart with the same state directory resumes exactly
// where the previous run stopped.
const (
	RecSubscription = "subscription"
	RecEvent        = "event"
	RecOwner        = "owner"
	RecDecision     = "decision"
	RecWatermark    = "watermark"
	RecKeyedSub      = "keyed_sub"      // a business-key ordered event bus exists
	RecKeyedEvent    = "keyed_event"    // one event's progress inside a keyed bus
	RecKeyedEvidence = "keyed_evidence" // late reply kept as evidence, never applied
	RecKeyedCancel   = "keyed_cancel"   // the bus subscription was cancelled
)

// SwitchDecision is the durable record of how a prepared upgrade was
// settled: which dispositions the user applied (migrate / discard /
// stop_subscription) for which extension and subscription.
type SwitchDecision struct {
	Time      time.Time              `json:"time"`
	UpgradeID string                 `json:"upgrade_id"`
	Decisions map[string]Disposition `json:"decisions"`
	Detail    string                 `json:"detail,omitempty"`
}

// RecoveryRecord is one line in the recovery log. Only the fields
// matching Kind are populated.
type RecoveryRecord struct {
	Kind      string `json:"kind"`
	Extension string `json:"extension"`

	Subscription *protocol.SubscriptionScope `json:"subscription,omitempty"`
	Event        *protocol.EventProgress     `json:"event,omitempty"`

	Session string            `json:"session,omitempty"`
	Owner   *SessionOwnership `json:"owner,omitempty"`

	Decision *SwitchDecision `json:"decision,omitempty"`

	// Watermark: highest confirmed stream position for a subscription
	// at the moment a switch was confirmed.
	SubscriptionID    string `json:"subscription_id,omitempty"`
	ConfirmedPosition uint64 `json:"confirmed_position,omitempty"`

	// Keyed event bus records. Raw carries the JSON-encoded worker
	// Manifest so a host restart can relaunch the same handler binary.
	BusID         string            `json:"bus_id,omitempty"`
	Workers       int               `json:"workers,omitempty"`
	QueueLimit    int               `json:"queue_limit,omitempty"`
	Capability    string            `json:"capability,omitempty"`
	Raw           json.RawMessage   `json:"raw,omitempty"`
	KeyedEvent    *KeyedEventRec    `json:"keyed_event,omitempty"`
	KeyedEvidence *KeyedEvidenceRec `json:"keyed_evidence,omitempty"`
}

// KeyedEventRec is the durable shape of one event inside a keyed bus.
type KeyedEventRec struct {
	Key          string `json:"key"`
	EventID      string `json:"event_id"`
	Position     uint64 `json:"position"`
	Status       string `json:"status"`
	OwnerSession string `json:"owner_session,omitempty"`
	OwnerVersion string `json:"owner_version,omitempty"`
	Attempts     int    `json:"attempts,omitempty"`
	Detail       string `json:"detail,omitempty"`
}

// KeyedEvidenceRec preserves a reply that arrived from a generation no
// longer responsible for the key. It is evidence about the original
// event only; it can never confirm a newer event for the same key.
type KeyedEvidenceRec struct {
	Key          string    `json:"key"`
	EventID      string    `json:"event_id"`
	Position     uint64    `json:"position"`
	OwnerSession string    `json:"owner_session"`
	OwnerVersion string    `json:"owner_version"`
	Outcome      string    `json:"outcome"`
	Detail       string    `json:"detail,omitempty"`
	Time         time.Time `json:"time"`
}

// RecoveryLog appends durable host state to a JSON-lines file and
// replays it on startup. A torn final line (host died mid-write) is
// discarded and the file truncated back to the last complete record,
// so recovery never fails on an incomplete tail.
type RecoveryLog struct {
	mu     sync.Mutex
	f      *os.File
	enc    *json.Encoder
	closed bool

	subs      map[string]map[string]protocol.SubscriptionScope // ext -> sub id
	events    map[string]map[string]protocol.EventProgress     // ext -> event key
	owners    map[string]SessionOwnership                      // session token
	decisions []SwitchDecision
	watermark map[string]uint64 // ext|sub id -> confirmed position

	keyedSubs      map[string]keyedSubState
	keyedEvents    map[string]map[string]KeyedEventRec // bus id -> key|eventID
	keyedEvidence  map[string][]KeyedEvidenceRec
}

// KeyedSubRec is the durable description of a business-key event bus.
type KeyedSubRec struct {
	ID         string
	Workers    int
	QueueLimit int
	Capability string
	Manifest   Manifest
}

type keyedSubState struct {
	rec      KeyedSubRec
	cancelled bool
}

// OpenRecoveryLog opens (or creates) the recovery log at path, replays
// every complete record, and repairs a truncated tail left by a crash.
func OpenRecoveryLog(path string) (*RecoveryLog, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	l := &RecoveryLog{
		f:         f,
		subs:      map[string]map[string]protocol.SubscriptionScope{},
		events:    map[string]map[string]protocol.EventProgress{},
		owners:    map[string]SessionOwnership{},
		watermark: map[string]uint64{},
		keyedSubs:     map[string]keyedSubState{},
		keyedEvents:   map[string]map[string]KeyedEventRec{},
		keyedEvidence: map[string][]KeyedEvidenceRec{},
	}
	good, err := l.replay()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	// Drop a torn tail: a crash can leave a partial final line; every
	// record before it stays valid and the file continues from there.
	if err := f.Truncate(good); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("repair recovery log tail: %w", err)
	}
	if _, err := f.Seek(0, 2); err != nil {
		_ = f.Close()
		return nil, err
	}
	l.enc = json.NewEncoder(f)
	return l, nil
}

// replay decodes records until the first undecodable line and returns
// the offset of the last complete record.
func (l *RecoveryLog) replay() (int64, error) {
	if _, err := l.f.Seek(0, 0); err != nil {
		return 0, err
	}
	var good int64
	scanner := bufio.NewScanner(l.f)
	scanner.Buffer(make([]byte, 0, 1<<20), 16<<20)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			good++
			continue
		}
		var rec RecoveryRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			break // torn or corrupt tail: keep everything before it
		}
		l.apply(rec)
		good += int64(len(line)) + 1
	}
	if err := scanner.Err(); err != nil {
		return good, fmt.Errorf("read recovery log: %w", err)
	}
	return good, nil
}

// apply folds one replayed record into the in-memory view.
func (l *RecoveryLog) apply(rec RecoveryRecord) {
	switch rec.Kind {
	case RecSubscription:
		if rec.Subscription == nil {
			return
		}
		m := l.subs[rec.Extension]
		if m == nil {
			m = map[string]protocol.SubscriptionScope{}
			l.subs[rec.Extension] = m
		}
		m[rec.Subscription.ID] = *rec.Subscription
	case RecEvent:
		if rec.Event == nil {
			return
		}
		m := l.events[rec.Extension]
		if m == nil {
			m = map[string]protocol.EventProgress{}
			l.events[rec.Extension] = m
		}
		ev := *rec.Event
		key := eventKey(ev)
		cur, ok := m[key]
		if !ok || eventRank(ev.Status) > eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position > cur.Position) {
			m[key] = ev
		}
	case RecOwner:
		if rec.Owner != nil && rec.Session != "" {
			l.owners[rec.Session] = *rec.Owner
		}
	case RecDecision:
		if rec.Decision != nil {
			l.decisions = append(l.decisions, *rec.Decision)
		}
	case RecWatermark:
		key := rec.Extension + "|" + rec.SubscriptionID
		if rec.ConfirmedPosition > l.watermark[key] {
			l.watermark[key] = rec.ConfirmedPosition
		}
	case RecKeyedSub:
		if rec.BusID == "" || len(rec.Raw) == 0 {
			return
		}
		var mf Manifest
		if err := json.Unmarshal(rec.Raw, &mf); err != nil {
			return
		}
		l.keyedSubs[rec.BusID] = keyedSubState{rec: KeyedSubRec{
			ID: rec.BusID, Workers: rec.Workers, QueueLimit: rec.QueueLimit,
			Capability: rec.Capability, Manifest: mf,
		}}
	case RecKeyedCancel:
		if st, ok := l.keyedSubs[rec.BusID]; ok {
			st.cancelled = true
			l.keyedSubs[rec.BusID] = st
		} else {
			l.keyedSubs[rec.BusID] = keyedSubState{cancelled: true}
		}
	case RecKeyedEvent:
		if rec.KeyedEvent == nil {
			return
		}
		m := l.keyedEvents[rec.BusID]
		if m == nil {
			m = map[string]KeyedEventRec{}
			l.keyedEvents[rec.BusID] = m
		}
		ev := *rec.KeyedEvent
		key := ev.Key + "\x00" + ev.EventID
		m[key] = ev // append-only log: later records are the newer state
	case RecKeyedEvidence:
		if rec.KeyedEvidence != nil {
			l.keyedEvidence[rec.BusID] = append(l.keyedEvidence[rec.BusID], *rec.KeyedEvidence)
		}
	}
}

// append persists one record and folds it into the in-memory view.
func (l *RecoveryLog) append(rec RecoveryRecord) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed || l.enc == nil {
		return
	}
	l.apply(rec)
	_ = l.enc.Encode(&rec)
}

func (l *RecoveryLog) logSubscription(ext string, sub protocol.SubscriptionScope) {
	l.append(RecoveryRecord{Kind: RecSubscription, Extension: ext, Subscription: &sub})
}

func (l *RecoveryLog) logEvent(ext string, ev protocol.EventProgress) {
	l.append(RecoveryRecord{Kind: RecEvent, Extension: ext, Event: &ev})
}

func (l *RecoveryLog) logOwner(session string, owner SessionOwnership) {
	l.append(RecoveryRecord{Kind: RecOwner, Session: session, Owner: &owner})
}

func (l *RecoveryLog) logDecision(upgradeID string, decisions map[string]Disposition, detail string) {
	cp := map[string]Disposition{}
	for k, v := range decisions {
		cp[k] = v
	}
	l.append(RecoveryRecord{Kind: RecDecision, Decision: &SwitchDecision{
		Time:      time.Now().UTC(),
		UpgradeID: upgradeID,
		Decisions: cp,
		Detail:    detail,
	}})
}

func (l *RecoveryLog) logWatermark(ext, subID string, position uint64) {
	l.append(RecoveryRecord{
		Kind: RecWatermark, Extension: ext,
		SubscriptionID: subID, ConfirmedPosition: position,
	})
}

func (l *RecoveryLog) logKeyedSub(rec KeyedSubRec) {
	raw, _ := json.Marshal(rec.Manifest)
	l.append(RecoveryRecord{
		Kind: RecKeyedSub, BusID: rec.ID, Workers: rec.Workers,
		QueueLimit: rec.QueueLimit, Capability: rec.Capability, Raw: raw,
	})
}

func (l *RecoveryLog) logKeyedEvent(busID string, ev KeyedEventRec) {
	cp := ev
	l.append(RecoveryRecord{Kind: RecKeyedEvent, BusID: busID, KeyedEvent: &cp})
}

func (l *RecoveryLog) logKeyedEvidence(busID string, ev KeyedEvidenceRec) {
	cp := ev
	l.append(RecoveryRecord{Kind: RecKeyedEvidence, BusID: busID, KeyedEvidence: &cp})
}

func (l *RecoveryLog) logKeyedCancel(busID string) {
	l.append(RecoveryRecord{Kind: RecKeyedCancel, BusID: busID})
}

// KeyedSubscriptions returns the recovered description of every keyed
// event bus from previous runs, including buses that were cancelled.
func (l *RecoveryLog) KeyedSubscriptions() map[string]KeyedSubRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]KeyedSubRec{}
	for id, st := range l.keyedSubs {
		rec := st.rec
		rec.ID = id
		out[id] = rec
	}
	return out
}

// KeyedCancelled reports which bus ids were explicitly unsubscribed.
func (l *RecoveryLog) KeyedCancelled() map[string]bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := map[string]bool{}
	for id, st := range l.keyedSubs {
		if st.cancelled {
			out[id] = true
		}
	}
	return out
}

// KeyedEvents returns the recovered per-event ledger for one bus in
// key then stream-position order.
func (l *RecoveryLog) KeyedEvents(busID string) []KeyedEventRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	m := l.keyedEvents[busID]
	out := make([]KeyedEventRec, 0, len(m))
	for _, ev := range m {
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Position < out[j].Position
	})
	return out
}

// KeyedEvidence returns the preserved late-reply evidence for one bus.
func (l *RecoveryLog) KeyedEvidence(busID string) []KeyedEvidenceRec {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]KeyedEvidenceRec, len(l.keyedEvidence[busID]))
	copy(out, l.keyedEvidence[busID])
	return out
}

// Subscriptions returns the durable scopes recorded for an extension.
func (l *RecoveryLog) Subscriptions(ext string) []protocol.SubscriptionScope {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]protocol.SubscriptionScope, 0, len(l.subs[ext]))
	for _, sub := range l.subs[ext] {
		out = append(out, sub)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Events returns the event ledger recorded for an extension, ordered
// by subscription and stream position.
func (l *RecoveryLog) Events(ext string) []protocol.EventProgress {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]protocol.EventProgress, 0, len(l.events[ext]))
	for _, ev := range l.events[ext] {
		out = append(out, ev)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].SubscriptionID != out[j].SubscriptionID {
			return out[i].SubscriptionID < out[j].SubscriptionID
		}
		return out[i].Position < out[j].Position
	})
	return out
}

// Owners returns the final recorded ownership of every session token.
func (l *RecoveryLog) Owners() map[string]SessionOwnership {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]SessionOwnership, len(l.owners))
	for k, v := range l.owners {
		out[k] = v
	}
	return out
}

// Decisions returns every recorded switch decision in order.
func (l *RecoveryLog) Decisions() []SwitchDecision {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]SwitchDecision, len(l.decisions))
	copy(out, l.decisions)
	return out
}

// Watermark returns the highest confirmed position recorded for one
// subscription of an extension.
func (l *RecoveryLog) Watermark(ext, subID string) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.watermark[ext+"|"+subID]
}

// Close flushes and closes the log file.
func (l *RecoveryLog) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}
