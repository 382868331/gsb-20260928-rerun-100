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
}

// New creates an extension with the given name and semantic version.
func New(name, version string) *Extension {
	return &Extension{
		Name:       name,
		Version:    version,
		caps:       map[string]capability{},
		state:      map[string]protocol.StateItem{},
		knownState: map[string]bool{},
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
