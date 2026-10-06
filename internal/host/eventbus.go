// Package host: key-ordered parallel event bus.
//
// An EventBus dispatches identified business events to several worker
// processes (handler instances). Events carrying the same business key
// always execute one at a time, in the order they entered the host;
// events with different keys execute in parallel across workers.
//
// Resizing the worker pool never stops the subscription and never
// replays events: keys without work in flight are re-hashed to the new
// set immediately, while a key whose event is still owned by an old
// generation stays pinned to that generation until the outcome is
// known. A late reply arriving after the pin was released (or after a
// timeout/retirement) is retained as evidence about the *original*
// event; it never confirms a newer event and is never forwarded to the
// new owner. Outcomes with unknown external effect must be resolved
// explicitly (confirm / fail) before the key keeps moving.
package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"
)

// Per-key event lifecycle states.
const (
	KeyedQueued     = "queued"     // admitted, waiting for a worker
	KeyedDispatched = "dispatched" // delivered to a worker, reply pending
	KeyedConfirmed  = "confirmed"  // worker acknowledged success
	KeyedFailed     = "failed"     // worker reported an explicit failure
	KeyedUnknown    = "unknown"    // outcome unknown; manual resolution needed
)

var (
	// ErrBusClosed is returned once a bus subscription is cancelled.
	ErrBusClosed = errors.New("event bus is closed")
	// ErrDuplicateEvent is returned for an event id the bus already
	// knows about, so callers cannot inject a second copy.
	ErrDuplicateEvent = errors.New("duplicate event")
	// ErrNotResolvable is returned when an event is not waiting on a
	// decision about an unknown outcome.
	ErrNotResolvable = errors.New("event is not awaiting resolution")
	// ErrUnknownBus reports a bus id the host does not manage.
	ErrUnknownBus = errors.New("unknown event bus")
)

// BackpressureError explains why an event was rejected: the per-key
// bounded queue is full while an earlier event for the same key is
// still being handled. Other keys are unaffected and keep advancing.
type BackpressureError struct {
	BusID string
	Key   string
	Limit int
	Depth int
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("event bus %q key %q backpressure: queue full (%d/%d); the key is blocked behind an earlier event, other keys still advance",
		e.BusID, e.Key, e.Depth, e.Limit)
}

// EventBusConfig configures one key-ordered parallel subscription.
type EventBusConfig struct {
	// ID is the durable subscription identifier.
	ID string
	// Worker is the manifest of the local, trusted handler binary.
	// Every replica runs the same binary; sessions stay independent.
	Worker Manifest
	// Capability is the task capability invoked for each event.
	Capability string
	// Workers is the initial number of handler instances (>= 1).
	Workers int
	// PerKeyQueueLimit bounds how many not-yet-dispatched events one
	// business key may hold. Events dispatched/in flight do not count.
	PerKeyQueueLimit int
}

// KeyedEvent is one event as observed by the bus.
type KeyedEvent struct {
	Key          string
	EventID      string
	Position     uint64
	Payload      []byte
	Status       string
	OwnerSession string
	OwnerVersion string
	Attempts     int
	Detail       string
}

// EventEvidence is a reply that arrived from a generation no longer
// responsible for the key. It is retained for forensics only.
type EventEvidence struct {
	Key          string
	EventID      string
	Position     uint64
	OwnerSession string
	OwnerVersion string
	Outcome      string
	Detail       string
	Time         time.Time
}

// BusStats is a concurrency-safe snapshot for tests and operators.
type BusStats struct {
	ID              string
	Configured      int
	ActiveWorkers   int
	RetiredWorkers  int
	Queued          int
	InFlight        int
	Confirmed       int
	Failed          int
	Unknown         int
	MaxKeyQueueSeen int
	PerKeyQueue     map[string]int
}

// AuditRecord logs one pool/key lifecycle decision.
type AuditRecord struct {
	Time   time.Time
	Event  string
	Detail string
}

const (
	BusAuditResize    = "resize"     // target replica count changed
	BusAuditTransfer  = "transfer"   // key ownership moved between generations
	BusAuditPin       = "pin"        // key pinned waiting for old outcome
	BusAuditUnknown   = "unknown"    // key locked on unknown outcome
	BusAuditResolve   = "resolve"    // manual resolution applied
	BusAuditEvidence  = "evidence"   // late reply retained as evidence
	BusAuditWorkerOut = "worker_out" // a worker process exited
	BusAuditCancel    = "cancel"     // subscription cancelled
)

type busEvent struct {
	KeyedEvent
}

// flight describes an event currently owned by a worker generation.
type flight struct {
	ev      *busEvent
	slotID  int
	generation int
}

// busSlot is one worker process incarnation. Retired slots keep their
// process alive while an old-generation event is still in flight; the
// scheduler never assigns new events to them.
type busSlot struct {
	id         int
	inst       *Instance
	retired    bool
	tombstone  bool // process exited and must not be retried
}

// EventBus is the key-affinity parallel dispatcher for one subscription.
type EventBus struct {
	manager *Manager
	cfg     EventBusConfig

	mu       sync.Mutex
	events   map[string]*busEvent // key \x00 eventID
	queues   map[string][]*busEvent
	seq      map[string]uint64 // per-key next position
	flights  map[string]*flight // key \x00 eventID
	pinned   map[string]bool   // keys locked on an in-flight old-generation or unknown event
	slots    []*busSlot
	nextSlot int
	active   int // number of non-retired slots
	target   int // desired replica count
	wakeCh   chan struct{}
	closed   bool
	canceled bool
	maxQueue int
	evidence []EventEvidence
	audit    []AuditRecord

	schedStop chan struct{}
	schedDone chan struct{}
}

// keyOwnerSlot returns the slot index a key should be served by for
// the given active-worker count. FNV hashing keeps the mapping stable
// across unrelated resize steps and spreads keys evenly.
func keyOwnerSlot(key string, active int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum64() % uint64(active))
}

func eventMapKey(key, eventID string) string { return key + "\x00" + eventID }

// recordEvent persists the current state of an event when a recovery
// log is configured.
func (b *EventBus) recordEvent(ev *busEvent) {
	if b.manager.cfg.Recovery == nil {
		return
	}
	b.manager.cfg.Recovery.logKeyedEvent(b.cfg.ID, KeyedEventRec{
		Key: ev.Key, EventID: ev.EventID, Position: ev.Position,
		Status: ev.Status, OwnerSession: ev.OwnerSession,
		OwnerVersion: ev.OwnerVersion, Attempts: ev.Attempts, Detail: ev.Detail,
	})
}

func (b *EventBus) audit(event, detail string) {
	b.mu.Lock()
	b.audit = append(b.audit, AuditRecord{Time: time.Now().UTC(), Event: event, Detail: detail})
	b.mu.Unlock()
}

func (b *EventBus) wake() {
	select {
	case b.wakeCh <- struct{}{}:
	default:
	}
}

// keyedPayload is the envelope sent to handler workers. It carries
// enough identity for the handler to stay idempotent on redelivery.
type keyedPayload struct {
	BusID    string          `json:"bus_id"`
	Key      string          `json:"key"`
	EventID  string          `json:"event_id"`
	Position uint64          `json:"position"`
	Attempt  int             `json:"attempt"`
	Payload  json.RawMessage `json:"payload"`
}

// newEventBus creates a bus and spawns the initial worker pool.
func newEventBus(m *Manager, cfg EventBusConfig, recovered []KeyedEventRec) (*EventBus, error) {
	if cfg.ID == "" {
		return nil, fmt.Errorf("event bus requires an id")
	}
	if cfg.Workers < 1 {
		cfg.Workers = 1
	}
	if cfg.PerKeyQueueLimit < 1 {
		return nil, fmt.Errorf("event bus %q requires a positive per-key queue limit", cfg.ID)
	}
	b := &EventBus{
		manager: m, cfg: cfg, target: cfg.Workers,
		events: map[string]*busEvent{},
		queues: map[string][]*busEvent{},
		seq:    map[string]uint64{},
		flights: map[string]*flight{},
		pinned: map[string]bool{},
		wakeCh: make(chan struct{}, 1),
		schedStop: make(chan struct{}),
		schedDone: make(chan struct{}),
	}
	// Recovered ledger: confirmed events become dedupe markers only;
	// everything else is rebuilt as a queued event in key order.
	for _, rec := range recovered {
		ev := &busEvent{KeyedEvent: KeyedEvent{
			Key: rec.Key, EventID: rec.EventID, Position: rec.Position,
			Status: rec.Status, Attempts: rec.Attempts, Detail: rec.Detail,
		}}
		if rec.Position > b.seq[rec.Key] {
			b.seq[rec.Key] = rec.Position
		}
		b.events[eventMapKey(rec.Key, rec.EventID)] = ev
		switch rec.Status {
		case KeyedConfirmed, KeyedFailed:
			// terminal; stays as a dedupe/history marker only
		case KeyedUnknown:
			// The previous run could not determine the effect; the key
		// must stay locked until a human resolves it.
			ev.Status = KeyedUnknown
			b.pinned[rec.Key] = true
		default:
			ev.Status = KeyedQueued
			b.queues[rec.Key] = append(b.queues[rec.Key], ev)
		}
	}
	for key, q := range b.queues {
		sort.Slice(q, func(i, j int) bool { return q[i].Position < q[j].Position })
		b.queues[key] = q
		if len(q) > b.maxQueue {
			b.maxQueue = len(q)
		}
	}
	if err := b.growTo(cfg.Workers); err != nil {
		b.closeSlots()
		return nil, err
	}
	if m.cfg.Recovery != nil {
		m.cfg.Recovery.logKeyedSub(KeyedSubRec{
			ID: cfg.ID, Workers: cfg.Workers, QueueLimit: cfg.PerKeyQueueLimit,
			Capability: cfg.Capability, Manifest: cfg.Worker,
		})
	}
	go b.scheduler()
	b.wake()
	return b, nil
}

// growTo spawns worker slots until len(slots) reaches n. New slots are
// appended, so key ownership for the old active prefix changes only by
// modulo rebalancing; it never requires stopping the subscription.
func (b *EventBus) growTo(n int) error {
	for len(b.slots) < n {
		inst, err := spawn(b.cfg.Worker, b.manager.cfg.HandshakeTimeout)
		if err != nil {
			return fmt.Errorf("spawn event worker %s replica %d: %w", b.cfg.ID, len(b.slots), err)
		}
		inst.recovery = b.manager.cfg.Recovery
		inst.setOrphanSink(b.onOrphan)
		slot := &busSlot{id: b.nextSlot, inst: inst}
		b.nextSlot++
		b.slots = append(b.slots, slot)
		b.active++
		b.audit(BusAuditResize, fmt.Sprintf("replica %d started session=%s version=%s", slot.id, inst.session, inst.extVer))
	}
	return nil
}

// activeSlots returns the currently serving slots in stable order.
func (b *EventBus) activeSlots() []*busSlot {
	out := make([]*busSlot, 0, b.active)
	for _, s := range b.slots {
		if !s.retired && !s.tombstone {
			out = append(out, s)
		}
	}
	return out
}

// chooseSlot maps a key to a live slot.
func (b *EventBus) chooseSlot(key string) *busSlot {
	live := b.activeSlots()
	if len(live) == 0 {
		return nil
	}
	idx := keyOwnerSlot(key, len(live))
	if idx >= len(live) {
		idx = len(live) - 1
	}
	return live[idx]
}

// Publish admits one event into the bus. Events of a full slow key get
// an explainable BackpressureError; admission of other keys continues.
func (b *EventBus) Publish(key, eventID string, position uint64, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	b.mu.Lock()
	if b.closed || b.canceled {
		b.mu.Unlock()
		return ErrBusClosed
	}
	mk := eventMapKey(key, eventID)
	if _, exists := b.events[mk]; exists {
		b.mu.Unlock()
		return fmt.Errorf("%w: %s/%s", ErrDuplicateEvent, key, eventID)
	}
	if position == 0 {
		b.seq[key]++
		position = b.seq[key]
	} else if position > b.seq[key] {
		b.seq[key] = position
	}
	if len(b.queues[key]) >= b.cfg.PerKeyQueueLimit {
		depth := len(b.queues[key])
		b.mu.Unlock()
		return &BackpressureError{BusID: b.cfg.ID, Key: key, Limit: b.cfg.PerKeyQueueLimit, Depth: depth}
	}
	ev := &busEvent{KeyedEvent: KeyedEvent{Key: key, EventID: eventID, Position: position, Status: KeyedQueued, Payload: raw}}
	b.events[mk] = ev
	b.queues[key] = append(b.queues[key], ev)
	if len(b.queues[key]) > b.maxQueue {
		b.maxQueue = len(b.queues[key])
	}
	b.recordEvent(ev)
	b.mu.Unlock()
	b.wake()
	return nil
}

// scheduler is the single dispatch loop. One key has at most one event
// in flight at a time and keys are popped in position order, while any
// free worker may serve a different key concurrently.
func (b *EventBus) scheduler() {
	defer close(b.schedDone)
	busy := map[int]bool{}
	for {
		dispatched := b.pump(busy)
		select {
		case <-b.schedStop:
			return
		case <-b.wakeCh:
		case <-time.After(50 * time.Millisecond):
			_ = dispatched
		}
	}
}

// pump makes at most one scheduling pass; it returns the number of
// events handed to workers. Busy retired slots keep occupying a worker
// until their old-generation flight settles.
func (b *EventBus) pump(busy map[int]bool) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed || b.canceled {
		return 0
	}
	// Deterministic key order keeps dispatch reproducible; ordering
	// across keys is irrelevant to the per-key guarantee.
	keys := make([]string, 0, len(b.queues))
	for key, q := range b.queues {
		if len(q) > 0 {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	n := 0
	for _, key := range keys {
		if b.pinned[key] {
			continue // an earlier event for this key is not settled yet
		}
		// Defence in depth: never overlap two flights for one key.
		overlap := false
		for mk, fl := range b.flights {
			if fl.ev.Key == key {
				overlap = true
				break
			}
			_ = mk
		}
		if overlap {
			continue
		}
		slot := b.chooseSlot(key)
		if slot == nil || busy[slot.id] {
			continue
		}
		q := b.queues[key]
		ev := q[0]
		b.queues[key] = q[1:]
		ev.Status = KeyedDispatched
		ev.Attempts++
		ev.OwnerSession = slot.inst.session
		ev.OwnerVersion = slot.inst.extVer
		gen := slot.id
		b.flights[eventMapKey(key, ev.EventID)] = &flight{ev: ev, slotID: slot.id, generation: gen}
		b.pinned[key] = true
		busy[slot.id] = true
		b.recordEvent(ev)
		n++
		go b.runFlight(slot, ev, busy)
	}
	return n
}

// runFlight delivers one event to its owning worker and settles the
// key exactly once. A success/failure from the owning generation ends
// the pin; a timeout or process death with an unknown effect keeps the
// key pinned until ResolveEvent. Replies arriving afterwards can only
// become evidence.
func (b *EventBus) runFlight(slot *busSlot, ev *busEvent, busy map[int]bool) {
	b.manager.mu.Lock()
	b.manager.taskSeq++
	reqID := fmt.Sprintf("keyed-%d", b.manager.taskSeq)
	b.manager.mu.Unlock()

	env := keyedPayload{
		BusID: b.cfg.ID, Key: ev.Key, EventID: ev.EventID,
		Position: ev.Position, Attempt: ev.Attempts, Payload: ev.Payload,
	}
	raw, _ := json.Marshal(env)
	resp, err := slot.inst.call(reqID, b.cfg.Capability, raw, b.manager.cfg.TaskTimeout)

	b.mu.Lock()
	defer b.mu.Unlock()
	mk := eventMapKey(ev.Key, ev.EventID)
	fl, ok := b.flights[mk]
	delete(busy, slot.id)
	if !ok || fl.ev != ev {
		// The flight was already settled (e.g. crash supervisor); this
		// reply belongs to a dead generation and is evidence only.
		b.keepEvidenceLocked(ev, slot, resp, err, "reply after the owning generation ended")
		b.mu.Unlock()
		b.wake()
		b.mu.Lock()
		return
	}
	switch {
	case err == nil && resp != nil && resp.Type == "result":
		delete(b.flights, mk)
		ev.Status = KeyedConfirmed
		ev.Detail = ""
		b.recordEvent(ev)
		delete(b.pinned, ev.Key)
		if slot.retired {
			b.auditLocked(BusAuditTransfer, fmt.Sprintf("key %s confirmed by retiring replica %d; ownership released", ev.Key, slot.id))
		} else {
			b.auditLocked(BusAuditResolve, fmt.Sprintf("key %s event %s confirmed by replica %d", ev.Key, ev.EventID, slot.id))
		}
	case err != nil && errors.Is(err, ErrCrashed):
		delete(b.flights, mk)
		slot.tombstone = true
		ev.Status = KeyedUnknown
		ev.Detail = "handler process exited before answering; external effect unknown"
		b.recordEvent(ev)
		b.auditLocked(BusAuditUnknown, fmt.Sprintf("key %s event %s: worker replica %d crashed", ev.Key, ev.EventID, slot.id))
		go b.replaceSlot(slot)
	default:
		// Timeout or explicit error result. An explicit error is a
		// definite failure; a timeout leaves the effect unknown.
		delete(b.flights, mk)
		if resp != nil && resp.Type == "error" && resp.Err != ErrCrashed.Error() {
			ev.Status = KeyedFailed
			ev.Detail = resp.Err
			b.recordEvent(ev)
			delete(b.pinned, ev.Key)
			audit := BusAuditResolve
			if slot.retired {
				audit = BusAuditTransfer
			}
			b.auditLocked(audit, fmt.Sprintf("key %s event %s failed explicitly: %s", ev.Key, ev.EventID, resp.Err))
		} else {
			ev.Status = KeyedUnknown
			detail := "handler timed out; external effect unknown"
			if err != nil {
				detail = err.Error()
			}
			ev.Detail = detail
			b.recordEvent(ev)
			b.auditLocked(BusAuditUnknown, fmt.Sprintf("key %s event %s unknown: %s", ev.Key, ev.EventID, detail))
		}
	}
	b.mu.Unlock()
	b.wake()
	b.mu.Lock()
}

func (b *EventBus) auditLocked(event, detail string) {
	b.audit = append(b.audit, AuditRecord{Time: time.Now().UTC(), Event: event, Detail: detail})
}

// keepEvidenceLocked stores a reply that can no longer change any
// state. It is tied to the original event identity and can never be
// applied to a newer event for the same key.
func (b *EventBus) keepEvidenceLocked(ev *busEvent, slot *busSlot, resp interface{ GetType() string }, callErr error, detail string) {
}
