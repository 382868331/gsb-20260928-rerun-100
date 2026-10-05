package host

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// JournalEntry records the outcome of a single task dispatch so every
// task can be traced back to the exact extension build that served it.
type JournalEntry struct {
	Time       time.Time `json:"time"`
	TaskID     string    `json:"task_id"`
	Extension  string    `json:"extension"`
	ExtVersion string    `json:"ext_version"`
	Session    string    `json:"session"`
	Capability string    `json:"capability"`
	Status     string    `json:"status"` // ok | error | timeout | crashed | rejected
	Detail     string    `json:"detail,omitempty"`
}

// SwitchEvent enumerates the lifecycle points recorded during an
// upgrade attempt. Every transition is persisted so a smooth handoff
// (or its rollback) can be replayed after the fact.
const (
	SwitchValidate    = "validate"     // candidate set passed topology checks
	SwitchPrepared    = "prepared"     // new instance passed handshake/cap checks
	SwitchStateCheck  = "state_check"  // old session state inspected, candidate compatibility known
	SwitchConfirmed   = "confirmed"    // user confirmed the migration and its dispositions
	SwitchCutover     = "cutover"      // new tasks now route to the new instance
	SwitchActivated   = "activated"    // old instance drained; new version live
	SwitchAbort       = "abort"        // new instance rejected before cutover
	SwitchRollback    = "rollback"     // candidate torn down, old version kept
	SwitchDrained     = "drained"      // old process exited after handing work off
	SwitchDrainKilled = "drain_killed" // old process exceeded drain boundary
	SwitchHostClose   = "host_close"   // wait ended because the host closed
)

// SwitchRecord is one entry in the handoff audit trail. OldVersion/
// OldSession are empty for extensions that had no running instance.
type SwitchRecord struct {
	Time       time.Time `json:"time"`
	Event      string    `json:"event"`
	UpgradeID  string    `json:"upgrade_id"`
	Extension  string    `json:"extension"`
	OldVersion string    `json:"old_version,omitempty"`
	OldSession string    `json:"old_session,omitempty"`
	NewVersion string    `json:"new_version,omitempty"`
	NewSession string    `json:"new_session,omitempty"`
	Detail     string    `json:"detail,omitempty"`
}

// Keyed-event evidence kinds recorded by KeyedExecutor.
const (
	KeyedReceived     = "received"
	KeyedBackpressure = "backpressure"
	KeyedDispatched   = "dispatched"
	KeyedConfirmed    = "confirmed"
	KeyedFailed       = "failed"
	KeyedUnknown      = "unknown_effect"
	KeyedLateReply    = "late_reply"
	KeyedResolved     = "resolved"
	KeyedRescale      = "rescale"
	KeyedWorkerExit   = "worker_exit"
)

// KeyedRecord is one entry in the keyed-subscription evidence trail.
// A late reply is recorded with the generation and session that
// produced it so it can be audited as evidence for the original event
// without ever confirming a newer dispatch.
type KeyedRecord struct {
	Time         time.Time `json:"time"`
	Subscription string    `json:"subscription"`
	Kind         string    `json:"kind"`
	EventID      string    `json:"event_id,omitempty"`
	Key          string    `json:"key,omitempty"`
	Seq          uint64    `json:"seq,omitempty"`
	Worker       int       `json:"worker,omitempty"`
	Session      string    `json:"session,omitempty"`
	Gen          uint64    `json:"gen,omitempty"`
	Detail       string    `json:"detail,omitempty"`
}

// Journal appends task records to a JSON-lines file.
type Journal struct {
	mu       sync.Mutex
	f        *os.File
	enc      *json.Encoder
	records  []JournalEntry
	switches []SwitchRecord
	keyed    []KeyedRecord
}

func OpenJournal(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Journal{enc: json.NewEncoder(f), f: f}, nil
}

// NewMemoryJournal keeps every record in process memory. It is used by
// tests and by applications that do not need a durable task log; the
// handoff audit trail must be inspectable even without a file.
func NewMemoryJournal() *Journal {
	return &Journal{}
}

func (j *Journal) Record(e JournalEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e.Time = time.Now().UTC()
	if j.enc == nil {
		j.records = append(j.records, e)
		return
	}
	_ = j.enc.Encode(&e)
}

// Switch appends one handoff audit record.
func (j *Journal) Switch(s SwitchRecord) {
	j.mu.Lock()
	defer j.mu.Unlock()
	s.Time = time.Now().UTC()
	j.switches = append(j.switches, s)
	if j.enc != nil {
		b, _ := json.Marshal(&s)
		_, _ = j.f.Write(append(b, '\n'))
	}
}

// SwitchRecords returns a copy of every handoff record observed.
func (j *Journal) SwitchRecords() []SwitchRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]SwitchRecord, len(j.switches))
	copy(out, j.switches)
	return out
}

// Keyed appends one keyed-subscription evidence record.
func (j *Journal) Keyed(r KeyedRecord) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r.Time = time.Now().UTC()
	j.keyed = append(j.keyed, r)
	if j.enc != nil {
		b, _ := json.Marshal(&r)
		_, _ = j.f.Write(append(b, '\n'))
	}
}

// KeyedRecords returns a copy of every keyed-subscription record.
func (j *Journal) KeyedRecords() []KeyedRecord {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]KeyedRecord, len(j.keyed))
	copy(out, j.keyed)
	return out
}

// TaskRecords returns a copy of every task record observed.
func (j *Journal) TaskRecords() []JournalEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	out := make([]JournalEntry, len(j.records))
	copy(out, j.records)
	return out
}

func (j *Journal) Close() error { return j.f.Close() }
