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

// Subscription and event-progress message types used during upgrades.
const (
	TypeSubscriptionsQuery       = "subscriptions_query"
	TypeSubscriptionsData        = "subscriptions_data"
	TypeSubscriptionsCheck       = "subscriptions_check"
	TypeSubscriptionsCheckResult = "subscriptions_check_result"
	TypeSubscriptionsLoad        = "subscriptions_load"
	TypeSubscriptionsLoaded      = "subscriptions_loaded"
	TypeEventProgress            = "event_progress"
	TypeEventsLoad               = "events_load"
	TypeEventsLoaded             = "events_loaded"
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

// SubscriptionScope identifies one durable stream subscription.
type SubscriptionScope struct {
	ID     string `json:"id"`
	Source string `json:"source"`
	Cursor string `json:"cursor,omitempty"`
}

// EventProgress tracks one identified stream event through a subscription.
type EventProgress struct {
	SubscriptionID string `json:"subscription_id"`
	EventID        string `json:"event_id"`
	Position       uint64 `json:"position"`
	Status         string `json:"status"`
	OwnerVersion   string `json:"owner_version,omitempty"`
	OwnerSession   string `json:"owner_session,omitempty"`
}

// Message is the single envelope used in both directions. Fields are
// populated according to Type.
type Message struct {
	Type          string              `json:"type"`
	Protocol      int                 `json:"protocol,omitempty"`
	Session       string              `json:"session,omitempty"`
	Name          string              `json:"name,omitempty"`
	ExtVer        string              `json:"ext_version,omitempty"`
	Caps          []Capability        `json:"capabilities,omitempty"`
	ID            string              `json:"id,omitempty"`
	Cap           string              `json:"capability,omitempty"`
	Payload       json.RawMessage     `json:"payload,omitempty"`
	Err           string              `json:"error,omitempty"`
	Items         []StateItem         `json:"items,omitempty"`
	Accepted      []string            `json:"accepted,omitempty"`
	Rejected      []string            `json:"rejected,omitempty"`
	Subscriptions []SubscriptionScope `json:"subscriptions,omitempty"`
	Events        []EventProgress     `json:"events,omitempty"`
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
