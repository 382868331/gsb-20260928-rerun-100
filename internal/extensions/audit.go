package extensions

import (
	"encoding/json"
	"os"
	"sync"
)

type JSONAuditor struct {
	mu   sync.Mutex
	file *os.File
}

func NewJSONAuditor(path string) (*JSONAuditor, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &JSONAuditor{file: file}, nil
}

func (a *JSONAuditor) Record(event TaskEvent) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	data = append(data, '\n')
	a.mu.Lock()
	defer a.mu.Unlock()
	_, _ = a.file.Write(data)
}

func (a *JSONAuditor) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.file.Close()
}
