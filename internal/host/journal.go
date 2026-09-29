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

// Journal appends task records to a JSON-lines file.
type Journal struct {
	mu  sync.Mutex
	enc *json.Encoder
	f   *os.File
}

func OpenJournal(path string) (*Journal, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Journal{enc: json.NewEncoder(f), f: f}, nil
}

func (j *Journal) Record(e JournalEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e.Time = time.Now().UTC()
	_ = j.enc.Encode(&e)
}

func (j *Journal) Close() error { return j.f.Close() }
