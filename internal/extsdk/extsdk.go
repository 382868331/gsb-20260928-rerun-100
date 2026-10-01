// Package extsdk is the extension-side helper. An extension registers
// capability handlers and calls Run; the SDK performs the handshake,
// dispatches tasks, and handles graceful shutdown.
package extsdk

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

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
	// OnShutdown, if set, runs after the last accepted task has
	// finished and before the bye acknowledgement is sent. It models
	// the extension's own drain/cleanup work; a slow hook lets a host
	// exercise the agreed drain boundary.
	OnShutdown func()
}

// New creates an extension with the given name and semantic version.
func New(name, version string) *Extension {
	return &Extension{Name: name, Version: version, caps: map[string]capability{}}
}

// Handle registers a handler for a capability at the given version.
func (e *Extension) Handle(name string, version int, h Handler) {
	e.caps[name] = capability{version: version, handler: h}
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
		case protocol.TypeShutdown:
			// Stop accepting new work; nothing else is in flight
			// because dispatch is sequential. Run any extension-owned
			// cleanup, then acknowledge and exit.
			if e.OnShutdown != nil {
				e.OnShutdown()
			}
			_ = protocol.Encode(os.Stdout, &protocol.Message{
				Type:    protocol.TypeBye,
				Session: session,
			})
			// Handshake the actual exit: flush the bye, then wait for
			// the host to close stdin so it is guaranteed to have
			// observed the bye line before the process vanishes.
			_ = os.Stdout.Sync()
			io.Copy(io.Discard, os.Stdin)
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
