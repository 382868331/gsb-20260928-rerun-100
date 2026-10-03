// Package extsdk is the extension-side helper. An extension registers
// capability handlers and calls Run; the SDK performs the handshake,
// dispatches tasks, and handles graceful shutdown.
package extsdk

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"

	"extsys/internal/protocol"
)

// Handler executes one task and returns a JSON-marshalable result.
type Handler func(payload json.RawMessage) (any, error)

type capability struct {
	version int
	handler Handler
}

// Extension describes a loadable extension process.
type Extension struct {
	Name    string
	Version string
	caps    map[string]capability

	outMu      sync.Mutex // serialises writes to stdout
	stateMu    sync.Mutex
	state      map[string]protocol.StateItem
	stateSeq   uint64
	knownState map[string]bool
	session    string

	subMu sync.Mutex
	subs  map[string]*subscription
}

// subscription is one declared event stream. NextID is the ID the next
// emitted event will carry; the host moves it via sub_load when another
// incarnation takes over, so the stream resumes exactly after the last
// externally confirmed event instead of replaying it.
type subscription struct {
	name   string
	nextID uint64
	active bool
}

// New creates an extension with the given name and semantic version.
func New(name, version string) *Extension {
	return &Extension{
		Name:       name,
		Version:    version,
		caps:       map[string]capability{},
		state:      map[string]protocol.StateItem{},
		knownState: map[string]bool{},
		subs:       map[string]*subscription{},
	}
}

// Handle registers a handler for a capability at the given version.
func (e *Extension) Handle(name string, version int, h Handler) {
	e.caps[name] = capability{version: version, handler: h}
}

// DeclareStateKeys registers the state keys this build can interpret.
// During an upgrade the host asks the candidate build to check the old
// session's saved state; keys not declared here are reported back as
// rejected so the user can decide how to dispose of them.
func (e *Extension) DeclareStateKeys(keys ...string) {
	for _, k := range keys {
		e.knownState[k] = true
	}
}

// DeclareSubscriptions registers the persistent subscriptions this
// build can produce and resume. During an upgrade the host only hands
// a subscription to a candidate that declared it; anything else shows
// up in the preview as not resumable and forces an explicit decision.
func (e *Extension) DeclareSubscriptions(names ...string) {
	e.subMu.Lock()
	defer e.subMu.Unlock()
	for _, n := range names {
		if _, ok := e.subs[n]; !ok {
			e.subs[n] = &subscription{name: n, nextID: 1, active: true}
		}
	}
}

// Emit assigns the next event ID for the subscription and delivers the
// event to the host. It fails when the subscription is undeclared or
// has been stopped (by a user decision during an upgrade).
func (e *Extension) Emit(sub string, payload any) (uint64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	e.subMu.Lock()
	s, ok := e.subs[sub]
	if !ok {
		e.subMu.Unlock()
		return 0, fmt.Errorf("undeclared subscription %q", sub)
	}
	if !s.active {
		e.subMu.Unlock()
		return 0, fmt.Errorf("subscription %q is stopped", sub)
	}
	id := s.nextID
	s.nextID++
	e.subMu.Unlock()
	session := e.session
	if session == "" {
		return id, nil
	}
	err = e.send(&protocol.Message{
		Type:    protocol.TypeSubEvent,
		Session: session,
		Events:  []protocol.SubEvent{{Sub: sub, ID: id, Payload: raw}},
	})
	return id, err
}

// SetState records one state item and notifies the host. Each update
// carries a fresh monotonic version, so a newer value always wins over
// anything an older incarnation may still be pushing.
func (e *Extension) SetState(key string, value any) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	e.stateMu.Lock()
	e.stateSeq++
	item := protocol.StateItem{Key: key, Value: raw, Version: e.stateSeq}
	e.state[key] = item
	session := e.session
	e.stateMu.Unlock()
	if session == "" {
		return nil
	}
	return e.send(&protocol.Message{
		Type:    protocol.TypeStatePush,
		Session: session,
		Items:   []protocol.StateItem{item},
	})
}

// State returns the current value of a state key.
func (e *Extension) State(key string) (json.RawMessage, bool) {
	e.stateMu.Lock()
	defer e.stateMu.Unlock()
	item, ok := e.state[key]
	return item.Value, ok
}

func (e *Extension) send(m *protocol.Message) error {
	e.outMu.Lock()
	defer e.outMu.Unlock()
	return protocol.Encode(os.Stdout, m)
}

// Run performs the handshake on stdin, serves tasks until a shutdown
// message arrives, then exits. In-flight work is finished before the
// bye acknowledgement is sent (handlers run sequentially).
func (e *Extension) Run() error {
	dec := json.NewDecoder(os.Stdin)

	var hello protocol.Message
	if err := dec.Decode(&hello); err != nil {
		return fmt.Errorf("read hello: %w", err)
	}
	if hello.Type != protocol.TypeHello {
		return fmt.Errorf("expected hello, got %q", hello.Type)
	}
	if !protocol.Compatible(hello.Protocol) {
		return fmt.Errorf("unsupported protocol version %d", hello.Protocol)
	}
	session := hello.Session
	e.session = session

	ack := &protocol.Message{
		Type:     protocol.TypeHelloAck,
		Protocol: protocol.Version,
		Session:  session,
		Name:     e.Name,
		ExtVer:   e.Version,
	}
	for name, c := range e.caps {
		ack.Caps = append(ack.Caps, protocol.Capability{Name: name, Version: c.version})
	}
	e.subMu.Lock()
	for name := range e.subs {
		ack.SubNames = append(ack.SubNames, name)
	}
	e.subMu.Unlock()
	if err := protocol.Encode(os.Stdout, ack); err != nil {
		return fmt.Errorf("write hello_ack: %w", err)
	}

	for {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return fmt.Errorf("read message: %w", err)
		}
		// Ignore anything not belonging to this negotiated session.
		if msg.Session != session {
			continue
		}
		switch msg.Type {
		case protocol.TypeTask:
			e.dispatch(&msg)
		case protocol.TypeShutdown:
			// Stop accepting new work; nothing else is in flight
			// because dispatch is sequential. Acknowledge and exit.
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeBye,
				Session: session,
			})
			return nil
		case protocol.TypeStateQuery:
			e.stateMu.Lock()
			items := make([]protocol.StateItem, 0, len(e.state))
			for _, item := range e.state {
				items = append(items, item)
			}
			e.stateMu.Unlock()
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeStateData,
				Session: session,
				ID:      msg.ID,
				Items:   items,
			})
		case protocol.TypeStateCheck:
			resp := &protocol.Message{
				Type:    protocol.TypeStateCheckResult,
				Session: session,
				ID:      msg.ID,
			}
			for _, item := range msg.Items {
				if e.knownState[item.Key] {
					resp.Accepted = append(resp.Accepted, item.Key)
				} else {
					resp.Rejected = append(resp.Rejected, item.Key)
				}
			}
			_ = e.send(resp)
		case protocol.TypeStateLoad:
			e.stateMu.Lock()
			for _, item := range msg.Items {
				if cur, ok := e.state[item.Key]; !ok || item.Version > cur.Version {
					e.state[item.Key] = item
				}
				if item.Version > e.stateSeq {
					e.stateSeq = item.Version
				}
			}
			e.stateMu.Unlock()
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeStateLoaded,
				Session: session,
				ID:      msg.ID,
			})
		case protocol.TypeSubQuery:
			e.subMu.Lock()
			progress := make([]protocol.SubProgress, 0, len(e.subs))
			for _, s := range e.subs {
				if s.active {
					progress = append(progress, protocol.SubProgress{Sub: s.name, NextID: s.nextID})
				}
			}
			e.subMu.Unlock()
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeSubProgress,
				Session: session,
				ID:      msg.ID,
				Subs:    progress,
			})
		case protocol.TypeSubLoad:
			// Adopt the resume positions the host computed from its
			// authoritative ledger: re-emitting starts right after the
			// last externally confirmed event, so unconfirmed events are
			// delivered again (and deduplicated host-side) instead of
			// being silently treated as processed.
			e.subMu.Lock()
			for _, p := range msg.Subs {
				if s, ok := e.subs[p.Sub]; ok {
					s.nextID = p.NextID
					s.active = true
				}
			}
			e.subMu.Unlock()
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeSubLoaded,
				Session: session,
				ID:      msg.ID,
			})
		case protocol.TypeSubStop:
			e.subMu.Lock()
			if s, ok := e.subs[msg.Name]; ok {
				s.active = false
			}
			e.subMu.Unlock()
			_ = e.send(&protocol.Message{
				Type:    protocol.TypeSubStopped,
				Session: session,
				ID:      msg.ID,
				Name:    msg.Name,
			})
		}
	}
}

func (e *Extension) dispatch(msg *protocol.Message) {
	resp := &protocol.Message{Session: msg.Session, ID: msg.ID}
	cap, ok := e.caps[msg.Cap]
	if !ok {
		resp.Type = protocol.TypeError
		resp.Err = fmt.Sprintf("unknown capability %q", msg.Cap)
	} else if out, err := cap.handler(msg.Payload); err != nil {
		resp.Type = protocol.TypeError
		resp.Err = err.Error()
	} else {
		raw, merr := json.Marshal(out)
		if merr != nil {
			resp.Type = protocol.TypeError
			resp.Err = "marshal result: " + merr.Error()
		} else {
			resp.Type = protocol.TypeResult
			resp.Payload = raw
		}
	}
	_ = e.send(resp)
}
