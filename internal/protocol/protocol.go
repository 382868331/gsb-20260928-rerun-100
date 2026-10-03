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
)

// State migration message types used during upgrades.
const (
	TypeStateQuery       = "state_query"        // host -> ext: dump your saved session state
	TypeStateData        = "state_data"         // ext -> host: state snapshot (reply to state_query)
	TypeStatePush        = "state_push"         // ext -> host: unsolicited state change notification
	TypeStateCheck       = "state_check"        // host -> ext: which of these items can you interpret?
	TypeStateCheckResult = "state_check_result" // ext -> host: accepted/rejected item keys
	TypeStateLoad        = "state_load"         // host -> ext: adopt these state items
	TypeStateLoaded      = "state_loaded"       // ext -> host: state adopted
)

// Subscription message types. A stateful extension may additionally
// hold persistent subscriptions that keep producing identified events;
// the host tracks per-event progress so an upgrade can hand the stream
// over without losing or double-confirming events.
const (
	TypeSubEvent    = "sub_event"    // ext -> host: unsolicited event delivery
	TypeSubQuery    = "sub_query"    // host -> ext: report your subscription progress
	TypeSubProgress = "sub_progress" // ext -> host: per-subscription emit positions
	TypeSubLoad     = "sub_load"     // host -> ext: resume these subscriptions at these positions
	TypeSubLoaded   = "sub_loaded"   // ext -> host: resume positions adopted
	TypeSubStop     = "sub_stop"     // host -> ext: stop this subscription (user decision)
	TypeSubStopped  = "sub_stopped"  // ext -> host: subscription stopped
)

// Capability is a named, versioned operation an extension can perform.
type Capability struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// StateItem is one versioned key/value entry in an extension's saved
// session state. Version is a per-session monotonic sequence assigned
// by the extension that produced the value; a higher version always
// wins during a merge, so a stale process can never overwrite a newer
// value that another incarnation already accepted.
type StateItem struct {
	Key     string          `json:"key"`
	Value   json.RawMessage `json:"value"`
	Version uint64          `json:"version"`
}

// SubEvent is one identified event delivered by a subscription. IDs are
// assigned by the producing extension as a per-subscription monotonic
// sequence, so the same event keeps its identity across versions and
// duplicate deliveries can be recognised.
type SubEvent struct {
	Sub     string          `json:"sub"`
	ID      uint64          `json:"id"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// SubProgress describes one subscription's emit position. NextID is the
// ID the extension will assign to the next event it emits; every ID
// below it has already been emitted (and the host either saw it or must
// treat the gap as uncertain).
type SubProgress struct {
	Sub    string `json:"sub"`
	NextID uint64 `json:"next_id"`
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
	SubNames []string        `json:"sub_names,omitempty"`
	ID       string          `json:"id,omitempty"`
	Cap      string          `json:"capability,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Err      string          `json:"error,omitempty"`
	Items    []StateItem     `json:"items,omitempty"`
	Events   []SubEvent      `json:"events,omitempty"`
	Subs     []SubProgress   `json:"subs,omitempty"`
	Accepted []string        `json:"accepted,omitempty"`
	Rejected []string        `json:"rejected,omitempty"`
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
