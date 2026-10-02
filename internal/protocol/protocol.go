// Package protocol defines the wire format exchanged between the host
// application and extension subprocesses. Messages are newline-delimited
// JSON carried over the child process's stdin/stdout.
package protocol

import (
	"encoding/json"
	"fmt"
	"io"
)

// Version is the protocol version implemented by this build. Host and
// extension must agree on the major version during the hello handshake.
const Version = 1

// Message types.
const (
	TypeHello    = "hello"     // host -> ext: protocol version + session token
	TypeHelloAck = "hello_ack" // ext -> host: identity + capabilities
	TypeTask     = "task"      // host -> ext: work request
	TypeResult   = "result"    // ext -> host: success
	TypeError    = "error"     // ext -> host: failure
	TypeShutdown = "shutdown"  // host -> ext: stop accepting, drain, exit
	TypeBye      = "bye"       // ext -> host: drained, about to exit

	// Session-state migration messages. The host uses these while an
	// upgrade candidate is being prepared: snapshot the running
	// session's state, ask the candidate which items it can interpret,
	// then apply the accepted items before cutover.
	TypeStateSnapshot = "state_snapshot" // host -> ext: report current session state
	TypeState         = "state"          // ext -> host: current state items
	TypeStateCheck    = "state_check"    // host -> ext: which of these items can you accept?
	TypeStateVerdict  = "state_verdict"  // ext -> host: accepted items + rejected keys with reasons
	TypeStateApply    = "state_apply"    // host -> ext: adopt these state items
	TypeStateApplied  = "state_applied"  // ext -> host: items adopted
)

// Capability is a named, versioned operation an extension can perform.
type Capability struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// Message is the single envelope used in both directions. Fields are
// populated according to Type.
type Message struct {
	Type     string          `json:"type"`
	Protocol int             `json:"protocol,omitempty"`
	Session  string          `json:"session,omitempty"`
	Name     string          `json:"name,omitempty"`
	ExtVer   string          `json:"ext_version,omitempty"`
	Caps     []Capability    `json:"capabilities,omitempty"`
	ID       string          `json:"id,omitempty"`
	Cap      string          `json:"capability,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Err      string          `json:"error,omitempty"`
}

// StateItems carries a session's state as opaque key/value items. The
// host never interprets the values; only the producing and consuming
// extension builds know their meaning.
type StateItems struct {
	Items map[string]json.RawMessage `json:"items"`
}

// StateVerdict is a candidate build's answer to a state check: the
// items it can accept unchanged, and the items it cannot interpret
// with a human-readable reason per key.
type StateVerdict struct {
	Accepted map[string]json.RawMessage `json:"accepted"`
	Rejected map[string]string          `json:"rejected"`
}

// Compatible reports whether a peer speaking protocol version v can
// interoperate with this build.
func Compatible(v int) bool { return v == Version }

// Encode writes one message as a JSON line.
func Encode(w io.Writer, m *Message) error {
	b, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("protocol: marshal: %w", err)
	}
	if _, err := w.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("protocol: write: %w", err)
	}
	return nil
}

// Decode reads one JSON-line message. Returns io.EOF on clean stream end.
func Decode(dec *json.Decoder, m *Message) error {
	return dec.Decode(m)
}
