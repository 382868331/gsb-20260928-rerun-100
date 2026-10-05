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
