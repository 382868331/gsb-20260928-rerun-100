package extensions

import "time"

const ProtocolVersion = 1

type Hello struct {
	Type         string   `json:"type"`
	Protocol     int      `json:"protocol"`
	Name         string   `json:"name"`
	Version      string   `json:"version"`
	Capabilities []string `json:"capabilities,omitempty"`
}

type Init struct {
	Type       string    `json:"type"`
	Protocol   int       `json:"protocol"`
	InstanceID string    `json:"instance_id"`
	SessionID  string    `json:"session_id"`
	Deadline   time.Time `json:"deadline,omitempty"`
}

type Request struct {
	Type       string    `json:"type"`
	ID         string    `json:"id"`
	Method     string    `json:"method"`
	Params     string    `json:"params,omitempty"`
	Deadline   time.Time `json:"deadline,omitempty"`
	Capability string    `json:"capability,omitempty"`
}

type Cancel struct {
	Type string `json:"type"`
	ID   string `json:"id"`
}

type Response struct {
	Type      string `json:"type"`
	ID        string `json:"id"`
	SessionID string `json:"session_id"`
	OK        bool   `json:"ok"`
	Result    string `json:"result,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	Error     string `json:"error,omitempty"`
}

type Shutdown struct {
	Type     string    `json:"type"`
	Deadline time.Time `json:"deadline"`
}

type Ready struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id"`
}
