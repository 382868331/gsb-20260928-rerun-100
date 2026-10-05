package host

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"extsys/internal/protocol"
)

// KeyedEvent is one stream event submitted to a keyed subscription.
// Events carrying the same Key are processed strictly in the order
// they entered the host; events with different keys may run in
// parallel on different processing instances.
type KeyedEvent struct {
	ID      string
	Key     string
	Cap     string // optional per-event capability override
	Payload any
}

// KeyedConfig tunes one keyed subscription executor.
type KeyedConfig struct {
	Name       string   // subscription name (durable identity)
	Manifest   Manifest // extension build run by every processing instance
	Capability string   // default capability invoked per event
	Instances  int      // initial number of processing instances
	// PerKeyQueueLimit bounds how many not-yet-dispatched events one
	// key may hold in memory. Submitting beyond it fails with a
	// *BackpressureError explaining which event is holding the key up;
	// other keys remain unaffected.
	PerKeyQueueLimit int
	// EventTimeout, when > 0, marks an event whose instance has not
	// answered in time as unknown-effect: the key stops advancing until
	// the event is confirmed, explicitly failed, or resolved by hand.
	EventTimeout     time.Duration
	HandshakeTimeout time.Duration
	ShutdownGrace    time.Duration
	Journal          *Journal
	// Recovery, when set, durably records ingress sequence numbers and
	// per-event progress so a host restart never re-executes a
	// confirmed event.
	Recovery *RecoveryLog
}

// BackpressureError explains why a submission was rejected: the key's
// bounded in-memory queue is full because an earlier event still
// occupies the key.
type BackpressureError struct {
	Key    string
	Queued int
	Limit  int
	Reason string
}

func (e *BackpressureError) Error() string {
	return fmt.Sprintf("key %q queue full (%d/%d queued): %s", e.Key, e.Queued, e.Limit, e.Reason)
}

// SubmitOutcome reports how one submission was classified.
type SubmitOutcome struct {
	Accepted  bool
	Duplicate bool // already confirmed, possibly by a previous host run
	Seq       uint64
}

// KeyedStats is a point-in-time view of the executor.
type KeyedStats struct {
	Instances     int
	Retiring      int
	Desired       int
	InFlight      int
	MaxConcurrent int
	QueuedTotal   int
	BlockedKeys   []string
	Generation    uint64
}

// KeyedCloseReport summarises what remained unconfirmed when the
// subscription was cancelled. Nothing is silently dropped: every
// unconfirmed event stays visible here and in the recovery log.
type KeyedCloseReport struct {
	Unconfirmed []string
}

// Manual resolution actions for an event with unknown effect.
const (
	ResolveRetry   = "retry"   // requeue the event at the key head
	ResolveConfirm = "confirm" // operator confirmed the effect happened
	ResolveDrop    = "drop"    // discard the event as explicitly failed
)

type queuedEvent struct {
	id      string
	key     string
	cap     string
	payload json.RawMessage
	seq     uint64
}

type inFlightEvent struct {
	ev    queuedEvent
	reqID string
	since time.Time
}

type keyedWorker struct {
	id       int
	inst     *Instance
	busy     bool
	retiring bool
	cur      *inFlightEvent
}

type keyState struct {
	queue    []queuedEvent
	inFlight bool
	blocked  bool
	reason   string
	unknown  *queuedEvent // event awaiting manual resolution
}

type lateReply struct {
	eventID string
	key     string
	gen     uint64
	session string
}

type executorState struct {
	keys      map[string]*keyState
	workers   []*keyedWorker
	late      map[string]lateReply
	confirmed map[string]uint64
	ingress   uint64
	reqSeq    uint64
	workerSeq int
	gen       uint64
	desired   int
	active    int
	maxActive int
	closing   bool
}

type keyedSubmit struct {
	ev   KeyedEvent
	resp chan submitResult
}

type submitResult struct {
	out SubmitOutcome
	err error
}

type keyedReplyMsg struct {
	w     *keyedWorker
	reqID string
	msg   *protocol.Message
}

type keyedRescaleReq struct {
	n    int
	resp chan error
}

type keyedResolveReq struct {
	key    string
	action string
	resp   chan error
}

type keyedConfirmReq struct {
	id   string
	resp chan bool
}

type keyedQueuedReq struct {
	key  string
	resp chan int
}

// KeyedExecutor runs one subscription's events across a rescalable
// pool of extension processes while preserving per-key order. All
// state transitions happen on a single internal goroutine; public
// methods exchange messages with it.
type KeyedExecutor struct {
	cfg KeyedConfig

	submitCh  chan keyedSubmit
	replyCh   chan keyedReplyMsg
	crashCh   chan *keyedWorker
	timeoutCh chan keyedReplyMsg
	rescaleCh chan keyedRescaleReq
	resolveCh chan keyedResolveReq
	statsCh   chan chan KeyedStats
	queuedCh  chan keyedQueuedReq
	confirmCh chan keyedConfirmReq
	closeCh   chan struct{}

	st          *executorState
	done        chan struct{}
	closeOnce   sync.Once
	closeReport KeyedCloseReport
	shutdownWg  sync.WaitGroup // outstanding async instance shutdowns
}

// NewKeyedExecutor starts the processing instance pool and restores
// any durable progress recorded under the same subscription name, so
// confirmed events from a previous host run are never re-executed.
func NewKeyedExecutor(cfg KeyedConfig) (*KeyedExecutor, error) {
	if cfg.Name == "" || cfg.Capability == "" {
		return nil, fmt.Errorf("keyed executor requires a name and a capability")
	}
	if cfg.Instances < 1 {
		return nil, fmt.Errorf("keyed executor requires at least one processing instance")
	}
	k := &KeyedExecutor{
		cfg:       cfg,
		submitCh:  make(chan keyedSubmit),
		replyCh:   make(chan keyedReplyMsg, 256),
		crashCh:   make(chan *keyedWorker, 128),
		timeoutCh: make(chan keyedReplyMsg, 256),
		rescaleCh: make(chan keyedRescaleReq),
		resolveCh: make(chan keyedResolveReq),
		statsCh:   make(chan chan KeyedStats),
		queuedCh:  make(chan keyedQueuedReq),
		confirmCh: make(chan keyedConfirmReq),
		closeCh:   make(chan struct{}),
		done:      make(chan struct{}),
	}
	k.st = &executorState{
		keys:      map[string]*keyState{},
		late:      map[string]lateReply{},
		confirmed: map[string]uint64{},
		desired:   cfg.Instances,
	}
	k.restore()
	for i := 0; i < cfg.Instances; i++ {
		if err := k.spawnWorker(); err != nil {
			for _, w := range k.st.workers {
				w.inst.shutdown(cfg.ShutdownGrace)
			}
			close(k.done)
			return nil, err
		}
	}
	go k.run()
	return k, nil
}

// Submit accepts one event into its key's bounded queue. A duplicate
// of an already-confirmed event is rejected as a no-op so a confirmed
// effect is never executed twice, even across a host restart.
func (k *KeyedExecutor) Submit(ev KeyedEvent) (SubmitOutcome, error) {
	resp := make(chan submitResult, 1)
	select {
	case k.submitCh <- keyedSubmit{ev: ev, resp: resp}:
		r := <-resp
		return r.out, r.err
	case <-k.done:
		return SubmitOutcome{}, fmt.Errorf("keyed executor %q is closed", k.cfg.Name)
	}
}

// Rescale changes the number of processing instances while the
// subscription keeps accepting events. Growing spawns fresh instances
// that immediately start draining queued keys; shrinking retires
// instances, letting each finish its in-flight event before exit.
// Keys are never pre-assigned to instances, so pending queues and key
// ownership transfer without stopping the subscription or replaying
// confirmed events.
func (k *KeyedExecutor) Rescale(n int) error {
	resp := make(chan error, 1)
	select {
	case k.rescaleCh <- keyedRescaleReq{n: n, resp: resp}:
		return <-resp
	case <-k.done:
		return fmt.Errorf("keyed executor %q is closed", k.cfg.Name)
	}
}

// ResolveKey settles an event whose effect is unknown (instance crash
// or timeout) so the key may advance again.
func (k *KeyedExecutor) ResolveKey(key, action string) error {
	resp := make(chan error, 1)
	select {
	case k.resolveCh <- keyedResolveReq{key: key, action: action, resp: resp}:
		return <-resp
	case <-k.done:
		return fmt.Errorf("keyed executor %q is closed", k.cfg.Name)
	}
}

// Stats returns the current executor snapshot.
func (k *KeyedExecutor) Stats() KeyedStats {
	resp := make(chan KeyedStats, 1)
	select {
	case k.statsCh <- resp:
		return <-resp
	case <-k.done:
		// The loop has exited; the state is now immutable, so the
		// final snapshot is safe to read.
		return k.snapshot(k.st)
	}
}

// Queued reports how many events of one key wait in memory.
func (k *KeyedExecutor) Queued(key string) int {
	resp := make(chan int, 1)
	select {
	case k.queuedCh <- keyedQueuedReq{key: key, resp: resp}:
		return <-resp
	case <-k.done:
		if ks, ok := k.st.keys[key]; ok {
			return len(ks.queue)
		}
		return 0
	}
}

// Confirmed reports whether an event ID has a durable confirmation.
func (k *KeyedExecutor) Confirmed(id string) bool {
	resp := make(chan bool, 1)
	select {
	case k.confirmCh <- keyedConfirmReq{id: id, resp: resp}:
		return <-resp
	case <-k.done:
		_, ok := k.st.confirmed[id]
		return ok
	}
}

// Close cancels the subscription: instances drain their in-flight
// event and exit, queues are released, and the report lists every
// event that was never confirmed.
func (k *KeyedExecutor) Close() KeyedCloseReport {
	k.closeOnce.Do(func() {
		select {
		case k.closeCh <- struct{}{}:
		case <-k.done:
		}
		<-k.done
	})
	return k.closeReport
}

// ---- event loop ----

func (k *KeyedExecutor) run() {
	st := k.st
	for {
		select {
		case s := <-k.submitCh:
			k.handleSubmit(st, s)
		case r := <-k.replyCh:
			k.handleReply(st, r)
		case w := <-k.crashCh:
			k.handleCrash(st, w)
		case t := <-k.timeoutCh:
			k.handleTimeout(st, t)
		case r := <-k.rescaleCh:
			r.resp <- k.handleRescale(st, r.n)
		case r := <-k.resolveCh:
			r.resp <- k.handleResolve(st, r.key, r.action)
		case resp := <-k.statsCh:
			resp <- k.snapshot(st)
		case q := <-k.queuedCh:
			n := 0
			if ks, ok := st.keys[q.key]; ok {
				n = len(ks.queue)
			}
			q.resp <- n
		case c := <-k.confirmCh:
			_, ok := st.confirmed[c.id]
			c.resp <- ok
		case <-k.closeCh:
			st.closing = true
			k.beginClose(st)
		}
		k.pump(st)
		if st.closing && len(st.workers) == 0 {
			k.closeReport = k.finishClose(st)
			// Retired instances shut down asynchronously; do not
			// report closed while a grace timer or kill is still
			// outstanding, or a slow process would be orphaned.
			k.shutdownWg.Wait()
			close(k.done)
			return
		}
	}
}

// restore reloads the durable event ledger recorded by a previous
// host run: confirmed events become dedupe markers and the ingress
// sequence continues above the highest recorded position.
func (k *KeyedExecutor) restore() {
	rec := k.cfg.Recovery
	if rec == nil {
		return
	}
	for _, ev := range rec.Events(k.recoveryExt()) {
		if ev.Position > k.st.ingress {
			k.st.ingress = ev.Position
		}
		if ev.Status == "confirmed" {
			k.st.confirmed[ev.EventID] = ev.Position
		}
	}
}

func (k *KeyedExecutor) recoveryExt() string { return "keyed/" + k.cfg.Name }

func (k *KeyedExecutor) spawnWorker() error {
	timeout := k.cfg.HandshakeTimeout
	if timeout == 0 {
		timeout = 2 * time.Second
	}
	inst, err := spawn(k.cfg.Manifest, timeout)
	if err != nil {
		return fmt.Errorf("keyed executor %q: spawn instance: %w", k.cfg.Name, err)
	}
	k.st.workerSeq++
	w := &keyedWorker{id: k.st.workerSeq, inst: inst}
	k.st.workers = append(k.st.workers, w)
	go func() {
		<-inst.done
		select {
		case k.crashCh <- w:
		default:
		}
	}()
	return nil
}

// removeWorker drops a worker from the pool.
func (st *executorState) removeWorker(w *keyedWorker) {
	out := st.workers[:0]
	for _, x := range st.workers {
		if x != w {
			out = append(out, x)
		}
	}
	st.workers = out
}

// ensureDesired tops the pool back up after a worker was lost, unless
// the executor is closing or a shrink is still draining.
func (k *KeyedExecutor) ensureDesired(st *executorState) {
	for !st.closing && len(st.workers) < st.desired {
		if err := k.spawnWorker(); err != nil {
			k.journal(KeyedRecord{Kind: KeyedWorkerExit, Detail: "replacement spawn failed: " + err.Error()})
			return
		}
	}
}

func (k *KeyedExecutor) handleSubmit(st *executorState, s keyedSubmit) {
	ev := s.ev
	if st.closing {
		s.resp <- submitResult{err: fmt.Errorf("keyed executor %q is closing", k.cfg.Name)}
		return
	}
	if ev.ID == "" || ev.Key == "" {
		s.resp <- submitResult{err: fmt.Errorf("event requires an ID and a key")}
		return
	}
	if _, dup := st.confirmed[ev.ID]; dup {
		s.resp <- submitResult{out: SubmitOutcome{Duplicate: true}}
		return
	}
	ks := st.keys[ev.Key]
	if ks == nil {
		ks = &keyState{}
		st.keys[ev.Key] = ks
	}
	if limit := k.cfg.PerKeyQueueLimit; limit > 0 && len(ks.queue) >= limit {
		bp := &BackpressureError{Key: ev.Key, Queued: len(ks.queue), Limit: limit, Reason: k.keyHoldReason(ks)}
		k.journal(KeyedRecord{Kind: KeyedBackpressure, EventID: ev.ID, Key: ev.Key, Detail: bp.Error()})
		s.resp <- submitResult{err: bp}
		return
	}
	payload, err := json.Marshal(ev.Payload)
	if err != nil {
		s.resp <- submitResult{err: err}
		return
	}
	capName := ev.Cap
	if capName == "" {
		capName = k.cfg.Capability
	}
	st.ingress++
	qe := queuedEvent{id: ev.ID, key: ev.Key, cap: capName, payload: payload, seq: st.ingress}
	ks.queue = append(ks.queue, qe)
	k.journal(KeyedRecord{Kind: KeyedReceived, EventID: ev.ID, Key: ev.Key, Seq: qe.seq})
	k.logProgress(qe, "received")
	s.resp <- submitResult{out: SubmitOutcome{Accepted: true, Seq: qe.seq}}
}

// keyHoldReason explains, in operator terms, why a key is not
// advancing: which event occupies it and why.
func (k *KeyedExecutor) keyHoldReason(ks *keyState) string {
	switch {
	case ks.blocked && ks.unknown != nil:
		return fmt.Sprintf("event %q has unknown effect (%s); resolve it before the key advances", ks.unknown.id, ks.reason)
	case ks.inFlight:
		return "head event still in flight on a processing instance"
	case ks.blocked:
		return "key blocked: " + ks.reason
	default:
		return "key is draining at its configured rate"
	}
}

// pump assigns queued head events to idle instances. A key offers its
// head only when nothing of it is in flight and it is not blocked, so
// per-key order is preserved no matter how the pool is rescaled.
func (k *KeyedExecutor) pump(st *executorState) {
	if st.closing {
		return
	}
	for {
		progressed := false
		for _, w := range st.workers {
			if w.busy || w.retiring {
				continue
			}
			qe, ks, ok := k.nextDispatchable(st)
			if !ok {
				return
			}
			k.dispatch(st, w, ks, qe)
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

func (k *KeyedExecutor) nextDispatchable(st *executorState) (queuedEvent, *keyState, bool) {
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ks := st.keys[key]
		if ks.blocked || ks.inFlight || len(ks.queue) == 0 {
			continue
		}
		return ks.queue[0], ks, true
	}
	return queuedEvent{}, nil, false
}

func (k *KeyedExecutor) dispatch(st *executorState, w *keyedWorker, ks *keyState, qe queuedEvent) {
	ks.queue = ks.queue[1:]
	st.reqSeq++
	reqID := fmt.Sprintf("%s-%d-%d", k.cfg.Name, st.gen, st.reqSeq)
	ch, err := w.inst.callAsync(reqID, qe.cap, qe.payload)
	if err != nil {
		// Process already gone before the task was written: the
		// event was never dispatched, so requeueing it is safe.
		// Retire the dead worker so pump does not spin on it.
		ks.queue = append([]queuedEvent{qe}, ks.queue...)
		k.retireWorker(st, w, KeyedWorkerExit, "instance unreachable at dispatch")
		k.ensureDesired(st)
		return
	}
	ks.inFlight = true
	w.busy = true
	w.cur = &inFlightEvent{ev: qe, reqID: reqID, since: time.Now()}
	st.active++
	if st.active > st.maxActive {
		st.maxActive = st.active
	}
	k.journal(KeyedRecord{Kind: KeyedDispatched, EventID: qe.id, Key: qe.key, Seq: qe.seq, Worker: w.id, Session: w.inst.session, Gen: st.gen})
	k.logProgress(qe, "processing")
	go func() {
		msg := <-ch
		select {
		case k.replyCh <- keyedReplyMsg{w: w, reqID: reqID, msg: msg}:
		default:
		}
	}()
	if k.cfg.EventTimeout > 0 {
		gen := st.gen
		time.AfterFunc(k.cfg.EventTimeout, func() {
			select {
			case k.timeoutCh <- keyedReplyMsg{w: w, reqID: reqID, msg: &protocol.Message{Err: fmt.Sprintf("generation %d", gen)}}:
			default:
			}
		})
	}
}

func (k *KeyedExecutor) handleReply(st *executorState, r keyedReplyMsg) {
	w := r.w
	if w.cur == nil || w.cur.reqID != r.reqID {
		k.noteLate(st, r)
		return
	}
	cur := w.cur
	w.cur = nil
	w.busy = false
	st.active--
	ks := st.keys[cur.ev.key]
	if ks != nil {
		ks.inFlight = false
	}
	switch {
	case r.msg.Type == protocol.TypeError && r.msg.Err == ErrCrashed.Error():
		// Process died before answering: effect unknown.
		k.markUnknown(st, w, cur, "instance exited before answering")
		k.retireWorker(st, w, KeyedWorkerExit, "instance exited")
		k.ensureDesired(st)
	case r.msg.Type == protocol.TypeError:
		k.journal(KeyedRecord{Kind: KeyedFailed, EventID: cur.ev.id, Key: cur.ev.key, Seq: cur.ev.seq, Worker: w.id, Session: w.inst.session, Gen: st.gen, Detail: r.msg.Err})
		k.logProgress(cur.ev, "failed")
		if w.retiring {
			k.retireWorker(st, w, KeyedWorkerExit, "retired after in-flight event")
		}
	default:
		st.confirmed[cur.ev.id] = cur.ev.seq
		k.journal(KeyedRecord{Kind: KeyedConfirmed, EventID: cur.ev.id, Key: cur.ev.key, Seq: cur.ev.seq, Worker: w.id, Session: w.inst.session, Gen: st.gen})
		k.logProgress(cur.ev, "confirmed")
		if w.retiring {
			k.retireWorker(st, w, KeyedWorkerExit, "retired after in-flight event")
		}
	}
	// If the process has already exited (e.g. it drained and quit
	// during close, and its exit notification was consumed while the
	// event was still in flight), release the worker now.
	if !w.busy && !w.inst.isAlive() && k.ownsWorker(st, w) {
		st.removeWorker(w)
		k.journal(KeyedRecord{Kind: KeyedWorkerExit, Worker: w.id, Session: w.inst.session, Gen: st.gen, Detail: "instance exited"})
		k.ensureDesired(st)
	}
}

// noteLate records a reply that no longer matches any live dispatch —
// produced by an older responsibility generation — as evidence for the
// original event. It never confirms the event a second time and never
// confirms whatever the key is processing now.
func (k *KeyedExecutor) noteLate(st *executorState, r keyedReplyMsg) {
	late, ok := st.late[r.reqID]
	if !ok {
		return
	}
	if r.msg.Type == protocol.TypeError && r.msg.Err == ErrCrashed.Error() {
		delete(st.late, r.reqID)
		return // crash of the old generation already recorded
	}
	delete(st.late, r.reqID)
	detail := "late result from retired generation kept as evidence only"
	if r.msg.Type == protocol.TypeError {
		detail = "late error from retired generation: " + r.msg.Err
	}
	k.journal(KeyedRecord{Kind: KeyedLateReply, EventID: late.eventID, Key: late.key, Gen: late.gen, Session: late.session, Detail: detail})
}

func (k *KeyedExecutor) handleCrash(st *executorState, w *keyedWorker) {
	if !k.ownsWorker(st, w) {
		return
	}
	if w.cur != nil {
		// The worker died with an event in flight. Its reply channel
		// already carries the final word (the real reply, or the
		// crash failure injected by failAll), so the reply handler
		// owns this transition; a second notification may follow.
		return
	}
	st.removeWorker(w)
	k.journal(KeyedRecord{Kind: KeyedWorkerExit, Worker: w.id, Session: w.inst.session, Gen: st.gen, Detail: "instance exited unexpectedly"})
	k.ensureDesired(st)
}

func (k *KeyedExecutor) handleTimeout(st *executorState, t keyedReplyMsg) {
	w := t.w
	if !k.ownsWorker(st, w) || w.cur == nil || w.cur.reqID != t.reqID {
		return // stale timer
	}
	cur := w.cur
	w.cur = nil
	w.busy = false
	st.active--
	if ks := st.keys[cur.ev.key]; ks != nil {
		ks.inFlight = false
	}
	k.markUnknown(st, w, cur, fmt.Sprintf("no reply within %s", k.cfg.EventTimeout))
	// The instance may still be working on the event; retire it so no
	// new event of any key is dispatched to a stale responsibility
	// generation. Its eventual reply lands in noteLate as evidence.
	k.retireWorker(st, w, KeyedWorkerExit, "retired after event timeout")
	k.ensureDesired(st)
}

// markUnknown blocks a key whose head event's effect is unknown. The
// key cannot advance — on any instance — until the event is confirmed,
// explicitly failed, or resolved by hand.
func (k *KeyedExecutor) markUnknown(st *executorState, w *keyedWorker, cur *inFlightEvent, reason string) {
	ks := st.keys[cur.ev.key]
	if ks == nil {
		return
	}
	ev := cur.ev
	ks.blocked = true
	ks.reason = reason
	ks.unknown = &ev
	st.late[cur.reqID] = lateReply{eventID: ev.id, key: ev.key, gen: st.gen, session: w.inst.session}
	k.journal(KeyedRecord{Kind: KeyedUnknown, EventID: ev.id, Key: ev.key, Seq: ev.seq, Worker: w.id, Session: w.inst.session, Gen: st.gen, Detail: reason})
}

// retireWorker removes a worker from the dispatch pool and shuts its
// process down; an in-flight reply that still arrives afterwards is
// handled through the late-reply evidence path.
func (k *KeyedExecutor) retireWorker(st *executorState, w *keyedWorker, kind, detail string) {
	if !k.ownsWorker(st, w) {
		return
	}
	st.removeWorker(w)
	k.journal(KeyedRecord{Kind: kind, Worker: w.id, Session: w.inst.session, Gen: st.gen, Detail: detail})
	grace := k.cfg.ShutdownGrace
	k.shutdownWg.Add(1)
	go func() {
		defer k.shutdownWg.Done()
		w.inst.shutdown(grace)
	}()
}

func (k *KeyedExecutor) ownsWorker(st *executorState, w *keyedWorker) bool {
	for _, x := range st.workers {
		if x == w {
			return true
		}
	}
	return false
}

func (k *KeyedExecutor) handleRescale(st *executorState, n int) error {
	if n < 1 {
		return fmt.Errorf("processing instance count must be at least 1")
	}
	if st.closing {
		return fmt.Errorf("keyed executor %q is closing", k.cfg.Name)
	}
	st.gen++
	from := len(st.workers)
	st.desired = n
	for len(st.workers) < n {
		if err := k.spawnWorker(); err != nil {
			st.desired = len(st.workers)
			return err
		}
	}
	if len(st.workers) > n {
		// Retire idle instances first; busy ones finish their
		// in-flight event (their key stays pinned to them) and are
		// retired by the reply handler.
		victims := len(st.workers) - n
		for _, w := range st.workers {
			if victims == 0 {
				break
			}
			if w.busy || w.retiring {
				continue
			}
			w.retiring = true
			victims--
		}
		for _, w := range st.workers {
			if victims == 0 {
				break
			}
			if w.retiring {
				continue
			}
			w.retiring = true // busy: drains its current event, then exits
			victims--
		}
		var retire []*keyedWorker
		for _, w := range st.workers {
			if w.retiring && !w.busy {
				retire = append(retire, w)
			}
		}
		for _, w := range retire {
			k.retireWorker(st, w, KeyedRescale, "scaled down; idle instance released")
		}
	}
	k.journal(KeyedRecord{Kind: KeyedRescale, Gen: st.gen, Detail: fmt.Sprintf("processing instances %d -> %d (generation %d)", from, n, st.gen)})
	return nil
}

func (k *KeyedExecutor) handleResolve(st *executorState, key, action string) error {
	ks := st.keys[key]
	if ks == nil || !ks.blocked || ks.unknown == nil {
		return fmt.Errorf("key %q has no event awaiting resolution", key)
	}
	ev := *ks.unknown
	switch action {
	case ResolveRetry:
		ks.queue = append([]queuedEvent{ev}, ks.queue...)
	case ResolveConfirm:
		st.confirmed[ev.id] = ev.seq
		k.logProgress(ev, "confirmed")
	case ResolveDrop:
		k.logProgress(ev, "failed")
	default:
		return fmt.Errorf("unknown resolution %q", action)
	}
	ks.blocked = false
	ks.reason = ""
	ks.unknown = nil
	k.journal(KeyedRecord{Kind: KeyedResolved, EventID: ev.id, Key: key, Seq: ev.seq, Gen: st.gen, Detail: "operator resolution: " + action})
	return nil
}

func (k *KeyedExecutor) snapshot(st *executorState) KeyedStats {
	out := KeyedStats{
		Instances:     len(st.workers),
		Desired:       st.desired,
		InFlight:      st.active,
		MaxConcurrent: st.maxActive,
		Generation:    st.gen,
	}
	for _, w := range st.workers {
		if w.retiring {
			out.Retiring++
		}
	}
	for key, ks := range st.keys {
		out.QueuedTotal += len(ks.queue)
		if ks.blocked {
			out.BlockedKeys = append(out.BlockedKeys, key)
		}
	}
	sort.Strings(out.BlockedKeys)
	return out
}

// beginClose stops every instance. In-flight events are allowed to
// confirm during the drain grace; their replies still flow through
// handleReply because the loop keeps running until the pool is empty.
func (k *KeyedExecutor) beginClose(st *executorState) {
	grace := k.cfg.ShutdownGrace
	for _, w := range st.workers {
		inst := w.inst
		k.shutdownWg.Add(1)
		go func() {
			defer k.shutdownWg.Done()
			inst.shutdown(grace)
			select {
			case k.crashCh <- w:
			default:
			}
		}()
	}
}

// finishClose releases the queues and reports everything that was
// never confirmed, so cancelling the subscription drops nothing
// silently.
func (k *KeyedExecutor) finishClose(st *executorState) KeyedCloseReport {
	report := KeyedCloseReport{}
	keys := make([]string, 0, len(st.keys))
	for key := range st.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		ks := st.keys[key]
		if ks.unknown != nil {
			report.Unconfirmed = append(report.Unconfirmed, ks.unknown.id)
		}
		for _, qe := range ks.queue {
			report.Unconfirmed = append(report.Unconfirmed, qe.id)
		}
		ks.queue = nil
	}
	return report
}

func (k *KeyedExecutor) journal(r KeyedRecord) {
	if k.cfg.Journal == nil {
		return
	}
	r.Subscription = k.cfg.Name
	k.cfg.Journal.Keyed(r)
}

func (k *KeyedExecutor) logProgress(ev queuedEvent, status string) {
	if k.cfg.Recovery == nil {
		return
	}
	k.cfg.Recovery.logEvent(k.recoveryExt(), protocol.EventProgress{
		SubscriptionID: k.cfg.Name,
		EventID:        ev.id,
		Position:       ev.seq,
		Status:         status,
	})
}
