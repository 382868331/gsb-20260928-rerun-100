package host

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"sync"
	"time"

	"extsys/internal/protocol"
)

// ErrCrashed is returned to pending callers when the extension process
// dies before answering them.
var ErrCrashed = errors.New("extension process exited")

type pendingCall struct {
	ch chan *protocol.Message
}

// Instance is one running extension process and its message channel.
type Instance struct {
	name    string
	session string
	extVer  string
	caps    map[string]int

	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{} // closed when the process exits

	mu      sync.Mutex
	pending map[string]*pendingCall
	alive   bool

	bye chan struct{} // closed when the extension sends bye

	stateMu          sync.Mutex
	pushedState      map[string]protocol.StateItem // async pushes, merged by version
	stateSink        func([]protocol.StateItem)    // set post-cutover to forward late pushes
	eventSink        func([]protocol.EventProgress)
	subscriptionSink func([]protocol.SubscriptionScope)
	subscriptions    map[string]protocol.SubscriptionScope
	events           map[string]protocol.EventProgress
}

// EndReason explains why a wait on an instance finished.
type EndReason string

const (
	EndExited    EndReason = "exited"     // process exited of its own accord
	EndBye       EndReason = "bye"        // drained and sent bye
	EndGraceKill EndReason = "grace_kill" // exceeded grace/drain boundary
	EndHostClose EndReason = "host_close" // host shutdown interrupted the wait
)

func newSessionToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// spawn starts the child process and performs the hello handshake.
// The negotiated session token is fresh for every process start, so a
// restarted process can never resume or impersonate a previous session.
func spawn(mf Manifest, handshakeTimeout time.Duration) (*Instance, error) {
	cmd := exec.Command(mf.Path, mf.Args...)
	if len(mf.Env) > 0 {
		cmd.Env = append(os.Environ(), mf.Env...)
	}
	cmd.Stderr = os.Stderr // extension diagnostics stay visible
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", mf.Path, err)
	}

	inst := &Instance{
		session:       newSessionToken(),
		cmd:           cmd,
		stdin:         stdin,
		done:          make(chan struct{}),
		pending:       map[string]*pendingCall{},
		alive:         true,
		bye:           make(chan struct{}),
		pushedState:   map[string]protocol.StateItem{},
		subscriptions: map[string]protocol.SubscriptionScope{},
		events:        map[string]protocol.EventProgress{},
	}

	hello := &protocol.Message{
		Type:     protocol.TypeHello,
		Protocol: protocol.Version,
		Session:  inst.session,
	}
	if err := protocol.Encode(stdin, hello); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("send hello: %w", err)
	}

	ackCh := make(chan *protocol.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		dec := json.NewDecoder(stdout)
		var ack protocol.Message
		if err := dec.Decode(&ack); err != nil {
			errCh <- err
			return
		}
		ackCh <- &ack
	}()

	select {
	case ack := <-ackCh:
		if ack.Type != protocol.TypeHelloAck {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("expected hello_ack, got %q", ack.Type)
		}
		if !protocol.Compatible(ack.Protocol) {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("incompatible protocol version %d", ack.Protocol)
		}
		if ack.Session != inst.session {
			_ = cmd.Process.Kill()
			return nil, errors.New("session token mismatch in hello_ack")
		}
		inst.name = ack.Name
		inst.extVer = ack.ExtVer
		inst.caps = map[string]int{}
		for _, c := range ack.Caps {
			inst.caps[c.Name] = c.Version
		}
	case err := <-errCh:
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("read hello_ack: %w", err)
	case <-time.After(handshakeTimeout):
		_ = cmd.Process.Kill()
		return nil, errors.New("handshake timed out")
	}

	go inst.serve(stdout)
	go func() {
		_ = cmd.Wait()
		inst.failAll(ErrCrashed)
		close(inst.done)
	}()
	return inst, nil
}

// serve reads results from the extension and routes them to callers.
func (inst *Instance) serve(stdout io.Reader) {
	dec := json.NewDecoder(stdout)
	for {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return // process gone; cmd.Wait handles cleanup
		}
		// Drop messages from any stale/foreign session: a restarted
		// process must not be able to answer for its predecessor.
		if msg.Session != inst.session {
			continue
		}
		if msg.Type == protocol.TypeBye {
			select {
			case <-inst.bye:
			default:
				close(inst.bye)
			}
			continue
		}
		if msg.Type == protocol.TypeStatePush {
			inst.noteStatePush(msg.Items)
			continue
		}
		if msg.Type == protocol.TypeSubscriptionsData {
			if msg.ID == "" {
				inst.noteSubscriptions(msg.Subscriptions)
				continue
			}
		}
		if msg.Type == protocol.TypeEventProgress {
			fresh := inst.noteEvents(msg.Events)
			inst.stateMu.Lock()
			sink := inst.eventSink
			inst.stateMu.Unlock()
			if len(fresh) > 0 && sink != nil {
				sink(fresh)
			}
			continue
		}
		inst.mu.Lock()
		pc, ok := inst.pending[msg.ID]
		if ok {
			delete(inst.pending, msg.ID)
		}
		inst.mu.Unlock()
		if ok {
			pc.ch <- &msg
		}
	}
}

func eventKey(ev protocol.EventProgress) string {
	return ev.SubscriptionID + "\x00" + ev.EventID
}

func (inst *Instance) noteSubscriptions(subs []protocol.SubscriptionScope) {
	inst.stateMu.Lock()
	for _, sub := range subs {
		inst.subscriptions[sub.ID] = sub
	}
	var sink func([]protocol.SubscriptionScope)
	sink = inst.subscriptionSink
	inst.stateMu.Unlock()
	if sink != nil {
		sink(subs)
	}
}

func (inst *Instance) noteEvents(events []protocol.EventProgress) []protocol.EventProgress {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	var fresh []protocol.EventProgress
	for _, ev := range events {
		if ev.OwnerVersion == "" {
			ev.OwnerVersion = inst.extVer
		}
		if ev.OwnerSession == "" {
			ev.OwnerSession = inst.session
		}
		key := eventKey(ev)
		cur, ok := inst.events[key]
		if !ok || eventRank(ev.Status) > eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position > cur.Position) {
			inst.events[key] = ev
			fresh = append(fresh, ev)
		}
	}
	return fresh
}

func eventRank(status string) int {
	switch status {
	case "received":
		return 1
	case "processing":
		return 2
	case "confirmed":
		return 3
	default:
		return 0
	}
}

// noteStatePush merges an unsolicited state update from the extension.
// Only a strictly newer version of a key replaces what the host has
// already seen, and any registered sink (a successor instance that has
// taken over the session) is notified so confirmed changes are not
// lost while a late duplicate can never roll state back.
func (inst *Instance) noteStatePush(items []protocol.StateItem) {
	inst.stateMu.Lock()
	var fresh []protocol.StateItem
	for _, item := range items {
		if cur, ok := inst.pushedState[item.Key]; !ok || item.Version > cur.Version {
			inst.pushedState[item.Key] = item
			fresh = append(fresh, item)
		}
	}
	sink := inst.stateSink
	inst.stateMu.Unlock()
	if sink != nil && len(fresh) > 0 {
		sink(fresh)
	}
}

// mergedState returns every state item the host knows about for this
// instance, combining async pushes with the given extra items by version.
func (inst *Instance) mergedState(extra []protocol.StateItem) []protocol.StateItem {
	merged := map[string]protocol.StateItem{}
	inst.stateMu.Lock()
	for k, v := range inst.pushedState {
		merged[k] = v
	}
	inst.stateMu.Unlock()
	for _, item := range extra {
		if cur, ok := merged[item.Key]; !ok || item.Version > cur.Version {
			merged[item.Key] = item
		}
	}
	out := make([]protocol.StateItem, 0, len(merged))
	for _, item := range merged {
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// setStateSink installs the forwarder for late state pushes.
func (inst *Instance) setStateSink(sink func([]protocol.StateItem)) {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	inst.stateSink = sink
}

func (inst *Instance) setEventSink(sink func([]protocol.EventProgress)) {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	inst.eventSink = sink
}

func (inst *Instance) setSubscriptionSink(sink func([]protocol.SubscriptionScope)) {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	inst.subscriptionSink = sink
}

func (inst *Instance) seedSubscriptions(subs []protocol.SubscriptionScope) {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	for _, sub := range subs {
		inst.subscriptions[sub.ID] = sub
	}
}

func (inst *Instance) seedEvents(events []protocol.EventProgress) {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	for _, ev := range events {
		k := eventKey(ev)
		cur := inst.events[k]
		if eventRank(ev.Status) > eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position > cur.Position) {
			inst.events[k] = ev
		}
	}
}

func (inst *Instance) adoptState(subscriptionID, eventID string, subs []protocol.SubscriptionScope, events []protocol.EventProgress, timeout time.Duration) error {
	if len(subs) > 0 {
		if err := inst.loadSubscriptions(subscriptionID, subs, timeout); err != nil {
			return err
		}
	}
	inst.seedSubscriptions(subs)
	if len(events) > 0 {
		if err := inst.loadEvents(eventID, events, timeout); err != nil {
			return err
		}
	}
	inst.seedEvents(events)
	return nil
}

// querySubscriptions returns durable subscription scopes and the current
// event ledger, including asynchronous updates received while a switch
// plan was open.
func (inst *Instance) querySubscriptions(id string, timeout time.Duration) ([]protocol.SubscriptionScope, []protocol.EventProgress, error) {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeSubscriptionsQuery, ID: id}, timeout)
	if err != nil {
		return nil, nil, err
	}
	if resp.Type != protocol.TypeSubscriptionsData {
		return nil, nil, fmt.Errorf("expected subscriptions_data, got %q", resp.Type)
	}
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	for _, sub := range resp.Subscriptions {
		inst.subscriptions[sub.ID] = sub
	}
	for _, ev := range resp.Events {
		k := eventKey(ev)
		cur, exists := inst.events[k]
		if !exists || eventRank(ev.Status) > eventRank(cur.Status) ||
			(eventRank(ev.Status) == eventRank(cur.Status) && ev.Position > cur.Position) {
			inst.events[k] = ev
		}
	}
	subs := make([]protocol.SubscriptionScope, 0, len(inst.subscriptions))
	for _, sub := range inst.subscriptions {
		subs = append(subs, sub)
	}
	eventByID := map[string]protocol.EventProgress{}
	for _, ev := range inst.events {
		eventByID[eventKey(ev)] = ev
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].ID < subs[j].ID })
	events := make([]protocol.EventProgress, 0, len(eventByID))
	for _, ev := range eventByID {
		events = append(events, ev)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].SubscriptionID != events[j].SubscriptionID {
			return events[i].SubscriptionID < events[j].SubscriptionID
		}
		return events[i].Position < events[j].Position
	})
	return subs, events, nil
}

func (inst *Instance) checkSubscriptions(id string, subs []protocol.SubscriptionScope, timeout time.Duration) ([]string, []string, error) {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeSubscriptionsCheck, ID: id, Subscriptions: subs}, timeout)
	if err != nil {
		return nil, nil, err
	}
	if resp.Type != protocol.TypeSubscriptionsCheckResult {
		return nil, nil, fmt.Errorf("expected subscriptions_check_result, got %q", resp.Type)
	}
	return resp.Accepted, resp.Rejected, nil
}

func (inst *Instance) loadSubscriptions(id string, subs []protocol.SubscriptionScope, timeout time.Duration) error {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeSubscriptionsLoad, ID: id, Subscriptions: subs}, timeout)
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeSubscriptionsLoaded {
		return fmt.Errorf("expected subscriptions_loaded, got %q", resp.Type)
	}
	return nil
}

func (inst *Instance) loadEvents(id string, events []protocol.EventProgress, timeout time.Duration) error {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeEventsLoad, ID: id, Events: events}, timeout)
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeEventsLoaded {
		return fmt.Errorf("expected events_loaded, got %q", resp.Type)
	}
	return nil
}

// adoptConfirmedEvent installs a confirmation produced by a previous owner
// unless this instance has already confirmed the duplicate itself.
func (inst *Instance) adoptConfirmedEvent(ev protocol.EventProgress) bool {
	inst.stateMu.Lock()
	defer inst.stateMu.Unlock()
	cur := inst.events[eventKey(ev)]
	if cur.Status == "confirmed" {
		return false
	}
	ev.Status = "confirmed"
	inst.events[eventKey(ev)] = ev
	return true
}

// request sends one message and waits for its correlated reply.
func (inst *Instance) request(msg *protocol.Message, timeout time.Duration) (*protocol.Message, error) {
	inst.mu.Lock()
	if !inst.alive {
		inst.mu.Unlock()
		return nil, ErrCrashed
	}
	pc := &pendingCall{ch: make(chan *protocol.Message, 1)}
	inst.pending[msg.ID] = pc
	inst.mu.Unlock()

	msg.Session = inst.session
	if err := protocol.Encode(inst.stdin, msg); err != nil {
		inst.mu.Lock()
		delete(inst.pending, msg.ID)
		inst.mu.Unlock()
		return nil, fmt.Errorf("send %s: %w", msg.Type, err)
	}
	select {
	case resp := <-pc.ch:
		return resp, nil
	case <-time.After(timeout):
		inst.mu.Lock()
		delete(inst.pending, msg.ID)
		inst.mu.Unlock()
		return nil, fmt.Errorf("%s timed out after %s", msg.Type, timeout)
	}
}

// queryState asks the extension for its saved session state and merges
// the reply with any async pushes the host has already observed.
func (inst *Instance) queryState(id string, timeout time.Duration) ([]protocol.StateItem, error) {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeStateQuery, ID: id}, timeout)
	if err != nil {
		return nil, err
	}
	if resp.Type != protocol.TypeStateData {
		return nil, fmt.Errorf("expected state_data, got %q", resp.Type)
	}
	return inst.mergedState(resp.Items), nil
}

// checkState asks the extension which of the given state items it can
// interpret. Returns (accepted, rejected) key lists.
func (inst *Instance) checkState(id string, items []protocol.StateItem, timeout time.Duration) ([]string, []string, error) {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeStateCheck, ID: id, Items: items}, timeout)
	if err != nil {
		return nil, nil, err
	}
	if resp.Type != protocol.TypeStateCheckResult {
		return nil, nil, fmt.Errorf("expected state_check_result, got %q", resp.Type)
	}
	return resp.Accepted, resp.Rejected, nil
}

// loadState hands state items to the extension for adoption.
func (inst *Instance) loadState(id string, items []protocol.StateItem, timeout time.Duration) error {
	resp, err := inst.request(&protocol.Message{Type: protocol.TypeStateLoad, ID: id, Items: items}, timeout)
	if err != nil {
		return err
	}
	if resp.Type != protocol.TypeStateLoaded {
		return fmt.Errorf("expected state_loaded, got %q", resp.Type)
	}
	return nil
}

func (inst *Instance) failAll(err error) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.alive = false
	for id, pc := range inst.pending {
		delete(inst.pending, id)
		pc.ch <- &protocol.Message{Type: protocol.TypeError, Err: err.Error()}
	}
}

// call sends one task and waits for the reply or ctx timeout.
func (inst *Instance) call(id, capability string, payload json.RawMessage, timeout time.Duration) (*protocol.Message, error) {
	return inst.request(&protocol.Message{
		Type:    protocol.TypeTask,
		ID:      id,
		Cap:     capability,
		Payload: payload,
	}, timeout)
}

// isAlive reports whether the process is still running.
func (inst *Instance) isAlive() bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return inst.alive
}

// inFlight reports how many tasks have been dispatched but not answered.
func (inst *Instance) inFlight() int {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return len(inst.pending)
}

// waitExit blocks until the process exits, the deadline elapses, or
// stop closes. It always returns an explicit reason for why the wait
// ended; deadline<=0 means "no drain boundary". When killed, pending
// callers are failed by the cmd.Wait goroutine.
func (inst *Instance) waitExit(deadline <-chan time.Time, stop <-chan struct{}) EndReason {
	select {
	case <-inst.done:
		return EndExited
	default:
	}
	select {
	case <-inst.done:
		return EndExited
	case <-inst.bye:
		select {
		case <-inst.done:
			return EndBye
		case <-deadlineOrNil(deadline):
			_ = inst.cmd.Process.Kill()
			<-inst.done
			return EndGraceKill
		case <-stopOrNil(stop):
			_ = inst.cmd.Process.Kill()
			<-inst.done
			return EndHostClose
		}
	case <-deadlineOrNil(deadline):
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return EndGraceKill
	case <-stopOrNil(stop):
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return EndHostClose
	}
}

func deadlineOrNil(c <-chan time.Time) <-chan time.Time {
	if c == nil {
		return nil
	}
	return c
}

func stopOrNil(c <-chan struct{}) <-chan struct{} {
	if c == nil {
		return nil
	}
	return c
}

// signalShutdown sends the shutdown directive when the instance is
// still alive.
func (inst *Instance) signalShutdown() bool {
	inst.mu.Lock()
	alive := inst.alive
	inst.mu.Unlock()
	if !alive {
		return false
	}
	_ = protocol.Encode(inst.stdin, &protocol.Message{
		Type:    protocol.TypeShutdown,
		Session: inst.session,
	})
	return true
}

// drain asks the old instance to stop accepting new work, then waits
// for in-flight tasks to finish and the process to exit. The grace
// duration is the agreed drain boundary; beyond it the process is
// killed. stop interrupts the wait with an explicit host-close reason.
func (inst *Instance) drain(grace time.Duration, stop <-chan struct{}) EndReason {
	if !inst.signalShutdown() {
		return EndExited
	}
	var deadline <-chan time.Time
	if grace > 0 {
		t := time.NewTimer(grace)
		defer t.Stop()
		deadline = t.C
	}
	return inst.waitExit(deadline, stop)
}

// shutdown asks the extension to stop accepting work and exit, then
// escalates to SIGKILL after the grace period.
func (inst *Instance) shutdown(grace time.Duration) {
	inst.drain(grace, nil)
}

// killNow terminates the process immediately and waits for cleanup.
func (inst *Instance) killNow() {
	inst.mu.Lock()
	alive := inst.alive
	inst.mu.Unlock()
	if !alive {
		return
	}
	_ = inst.cmd.Process.Kill()
	<-inst.done
}
