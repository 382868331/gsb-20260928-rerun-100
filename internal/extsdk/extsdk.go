// Package extsdk is the extension-side helper. An extension registers
// capability handlers and calls Run; the SDK performs the handshake,
// dispatches tasks, and handles graceful shutdown.
package extsdk

import (
	"encoding/json"
	"fmt"
	"os"

	"extsys/internal/protocol"
)

// Handler executes one task and returns a JSON-marshalable result.
type Handler func(payload json.RawMessage) (any, error)

// StateSnapshotter reports the extension's current session state as
// opaque key/value items. The host calls it during upgrade preparation
// so the state can be migrated to the candidate build.
type StateSnapshotter func() map[string]json.RawMessage

// StateChecker inspects state items produced by another build and
// reports which of them this build can accept unchanged and which it
// cannot interpret (with a reason per rejected key).
type StateChecker func(items map[string]json.RawMessage) (accepted map[string]json.RawMessage, rejected map[string]string)

// StateApplier adopts state items migrated from a previous build.
type StateApplier func(items map[string]json.RawMessage) error

type capability struct {
	version int
	handler Handler
}

// Extension describes a loadable extension process.
type Extension struct {
	Name    string
	Version string
	caps    map[string]capability

	snapshotter StateSnapshotter
	checker     StateChecker
	applier     StateApplier
}

// New creates an extension with the given name and semantic version.
func New(name, version string) *Extension {
	return &Extension{Name: name, Version: version, caps: map[string]capability{}}
}

// Handle registers a handler for a capability at the given version.
func (e *Extension) Handle(name string, version int, h Handler) {
	e.caps[name] = capability{version: version, handler: h}
}

// StateHooks registers the session-state migration hooks. All hooks
// are optional: without a snapshotter the session reports empty state,
// without a checker every item is accepted, and without an applier
// applied items are acknowledged and dropped.
func (e *Extension) StateHooks(s StateSnapshotter, c StateChecker, a StateApplier) {
	e.snapshotter, e.checker, e.applier = s, c, a
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
		case protocol.TypeStateSnapshot:
			e.replyState(&msg)
		case protocol.TypeStateCheck:
			e.replyVerdict(&msg)
		case protocol.TypeStateApply:
			e.replyApplied(&msg)
		case protocol.TypeShutdown:
			// Stop accepting new work; nothing else is in flight
			// because dispatch is sequential. Acknowledge and exit.
			_ = protocol.Encode(os.Stdout, &protocol.Message{
				Type:    protocol.TypeBye,
				Session: session,
			})
			return nil
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
	_ = protocol.Encode(os.Stdout, resp)
}

func (e *Extension) replyState(msg *protocol.Message) {
	items := map[string]json.RawMessage{}
	if e.snapshotter != nil {
		items = e.snapshotter()
	}
	payload, _ := json.Marshal(protocol.StateItems{Items: items})
	_ = protocol.Encode(os.Stdout, &protocol.Message{
		Type:    protocol.TypeState,
		Session: msg.Session,
		ID:      msg.ID,
		Payload: payload,
	})
}

func (e *Extension) replyVerdict(msg *protocol.Message) {
	resp := &protocol.Message{Type: protocol.TypeStateVerdict, Session: msg.Session, ID: msg.ID}
	var in protocol.StateItems
	if err := json.Unmarshal(msg.Payload, &in); err != nil {
		resp.Type = protocol.TypeError
		resp.Err = "decode state_check: " + err.Error()
	} else {
		accepted := map[string]json.RawMessage{}
		rejected := map[string]string{}
		if e.checker != nil {
			accepted, rejected = e.checker(in.Items)
		} else {
			for k, v := range in.Items {
				accepted[k] = v
			}
		}
		payload, _ := json.Marshal(protocol.StateVerdict{Accepted: accepted, Rejected: rejected})
		resp.Payload = payload
	}
	_ = protocol.Encode(os.Stdout, resp)
}

func (e *Extension) replyApplied(msg *protocol.Message) {
	resp := &protocol.Message{Type: protocol.TypeStateApplied, Session: msg.Session, ID: msg.ID}
	var in protocol.StateItems
	if err := json.Unmarshal(msg.Payload, &in); err != nil {
		resp.Type = protocol.TypeError
		resp.Err = "decode state_apply: " + err.Error()
	} else if e.applier != nil {
		if err := e.applier(in.Items); err != nil {
			resp.Type = protocol.TypeError
			resp.Err = err.Error()
		}
	}
	_ = protocol.Encode(os.Stdout, resp)
}
