package extensions

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"
)

var ErrUnavailable = errors.New("extension unavailable")

type TaskEvent struct {
	Time       time.Time `json:"time"`
	TaskID     string    `json:"task_id"`
	Extension  string    `json:"extension"`
	Version    string    `json:"version"`
	Generation int       `json:"generation"`
	SessionID  string    `json:"session_id"`
	Method     string    `json:"method"`
	Status     string    `json:"status"`
	DurationMS int64     `json:"duration_ms"`
	Error      string    `json:"error,omitempty"`
}

type Auditor interface {
	Record(TaskEvent)
}

type Host struct {
	executable string
	manifests  []Manifest
	audit      Auditor

	mu     sync.Mutex
	active map[string]*extensionProcess
	closed bool
}

type extensionProcess struct {
	executable string
	manifest   Manifest

	procMu     sync.Mutex
	cmd        *exec.Cmd
	stdin      io.WriteCloser
	encoder    *json.Encoder
	waitCh     chan error
	pending    map[string]chan Response
	generation int
	session    string

	callMu     sync.Mutex
	stopOnce   sync.Once
	stopped    chan struct{}
	restartMu  sync.Mutex
	restarting bool
}

func NewHost(auditor Auditor) *Host {
	return &Host{
		executable: os.Args[0],
		manifests:  BuiltinManifests(),
		audit:      auditor,
		active:     map[string]*extensionProcess{},
	}
}

func NewHostWithExecutable(executable string, auditor Auditor) *Host {
	host := NewHost(auditor)
	host.executable = executable
	return host
}

func (h *Host) Start() error {
	order, err := startupOrder(h.manifests)
	if err != nil {
		return err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrUnavailable
	}
	for _, manifest := range order {
		proc := newExtensionProcess(h.executable, manifest)
		if err := proc.start(); err != nil {
			for _, started := range h.active {
				started.stop(500 * time.Millisecond)
			}
			return fmt.Errorf("start %s: %w", manifest.Name, err)
		}
		h.active[manifest.Name] = proc
	}
	return nil
}

func (h *Host) Call(ctx context.Context, extensionName, method, params string, timeout time.Duration) (string, TaskEvent, error) {
	startedAt := time.Now()
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return "", TaskEvent{}, fmt.Errorf("%w: host is shutting down", ErrUnavailable)
	}
	proc := h.active[extensionName]
	h.mu.Unlock()
	if proc == nil {
		return "", TaskEvent{}, fmt.Errorf("%w: unknown extension %q", ErrUnavailable, extensionName)
	}

	proc.callMu.Lock()
	defer proc.callMu.Unlock()

	proc.procMu.Lock()
	generation := proc.generation
	session := proc.session
	version := proc.manifest.Version
	proc.procMu.Unlock()

	taskID := newToken("task")
	deadline := time.Now().Add(timeout)
	reqCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	responseCh, err := proc.send(Request{
		Type:       "request",
		ID:         taskID,
		Method:     method,
		Params:     params,
		Deadline:   deadline,
		Capability: capabilityForMethod(method),
	})
	if err != nil {
		event := makeEvent(taskID, extensionName, version, generation, session, method, "dispatch_failed", startedAt, err)
		h.record(event)
		return "", event, err
	}

	var response Response
	select {
	case response = <-responseCh:
		proc.removePending(taskID)
		if response.SessionID != session {
			err = errors.New("response came from a restarted extension session")
			break
		}
		if !response.OK {
			err = fmt.Errorf("%s: %s", response.ErrorCode, response.Error)
			if response.ErrorCode == "deadline_exceeded" {
				err = fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
			}
		} else {
			event := makeEvent(taskID, extensionName, version, generation, session, method, "completed", startedAt, nil)
			h.record(event)
			return response.Result, event, nil
		}
	case <-reqCtx.Done():
		err = fmt.Errorf("%w: %v", context.DeadlineExceeded, reqCtx.Err())
		_ = proc.sendControl(Cancel{Type: "cancel", ID: taskID})
		proc.removePending(taskID)
	case exitErr := <-proc.exitChannel():
		err = fmt.Errorf("extension %q exited: %w", extensionName, exitErr)
	}

	status := "failed"
	if errors.Is(err, context.DeadlineExceeded) {
		status = "timed_out"
	}
	event := makeEvent(taskID, extensionName, version, generation, session, method, status, startedAt, err)
	h.record(event)
	if status != "timed_out" {
		proc.handleFailure()
	}
	return "", event, err
}

func (h *Host) Close(timeout time.Duration) error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	procs := make([]*extensionProcess, 0, len(h.active))
	order, _ := startupOrder(h.manifests)
	for i := len(order) - 1; i >= 0; i-- {
		if proc := h.active[order[i].Name]; proc != nil {
			procs = append(procs, proc)
		}
	}
	h.mu.Unlock()

	var errs []error
	for _, proc := range procs {
		proc.callMu.Lock()
		defer proc.callMu.Unlock()
		if err := proc.stop(timeout / time.Duration(max(len(procs), 1))); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (h *Host) record(event TaskEvent) {
	if h.audit != nil {
		h.audit.Record(event)
	}
}

func newExtensionProcess(executable string, manifest Manifest) *extensionProcess {
	return &extensionProcess{
		executable: executable,
		manifest:   manifest,
		pending:    map[string]chan Response{},
		stopped:    make(chan struct{}),
	}
}

func (p *extensionProcess) start() error {
	p.procMu.Lock()
	defer p.procMu.Unlock()

	cmd := exec.Command(p.executable, "__extension-worker", p.manifest.Builtin)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}

	p.cmd = cmd
	p.stdin = stdin
	p.encoder = json.NewEncoder(stdin)
	p.waitCh = make(chan error, 1)
	p.generation++
	p.session = newToken("sess")

	initDeadline := time.Now().Add(3 * time.Second)
	if err := p.encoder.Encode(Init{
		Type:       "init",
		Protocol:   ProtocolVersion,
		InstanceID: p.manifest.Name + "-" + fmt.Sprint(p.generation),
		SessionID:  p.session,
		Deadline:   initDeadline,
	}); err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	decoder := json.NewDecoder(bufio.NewReader(stdout))
	var hello Hello
	if err := decoder.Decode(&hello); err != nil {
		_ = cmd.Process.Kill()
		return err
	}
	if hello.Protocol != ProtocolVersion || hello.Name != p.manifest.Name {
		_ = cmd.Process.Kill()
		return fmt.Errorf("incompatible extension handshake: protocol=%d name=%s", hello.Protocol, hello.Name)
	}
	if hello.Version != p.manifest.Version {
		_ = cmd.Process.Kill()
		return fmt.Errorf("extension version mismatch: expected %s, got %s", p.manifest.Version, hello.Version)
	}
	for _, capability := range p.manifest.Capabilities {
		if !contains(hello.Capabilities, capability) {
			_ = cmd.Process.Kill()
			return fmt.Errorf("extension missing negotiated capability %q", capability)
		}
	}
	var ready Ready
	if err := decoder.Decode(&ready); err != nil || ready.SessionID != p.session {
		_ = cmd.Process.Kill()
		return fmt.Errorf("invalid ready message")
	}

	go func() {
		for {
			var response Response
			if err := decoder.Decode(&response); err != nil {
				return
			}
			p.procMu.Lock()
			ch := p.pending[response.ID]
			p.procMu.Unlock()
			if ch != nil {
				ch <- response
			}
		}
	}()
	go func() {
		err := cmd.Wait()
		p.procMu.Lock()
		waitCh := p.waitCh
		pending := p.pending
		p.pending = map[string]chan Response{}
		session := p.session
		p.waitCh = nil
		p.procMu.Unlock()
		for id, ch := range pending {
			response := Response{
				Type:      "response",
				ID:        id,
				SessionID: session,
				OK:        false,
				ErrorCode: "extension_exited",
				Error:     errString(err),
			}
			select {
			case ch <- response:
			default:
			}
		}
		waitCh <- err
	}()
	return nil
}

func (p *extensionProcess) send(req Request) (chan Response, error) {
	p.procMu.Lock()
	defer p.procMu.Unlock()
	if p.waitCh == nil {
		return nil, fmt.Errorf("%w: process is not running", ErrUnavailable)
	}
	ch := make(chan Response, 1)
	p.pending[req.ID] = ch
	if err := p.encoder.Encode(req); err != nil {
		delete(p.pending, req.ID)
		return nil, err
	}
	return ch, nil
}

func (p *extensionProcess) sendControl(value any) error {
	p.procMu.Lock()
	defer p.procMu.Unlock()
	if p.encoder == nil {
		return ErrUnavailable
	}
	return p.encoder.Encode(value)
}

func (p *extensionProcess) removePending(id string) {
	p.procMu.Lock()
	defer p.procMu.Unlock()
	delete(p.pending, id)
}

func (p *extensionProcess) exitChannel() chan error {
	p.procMu.Lock()
	defer p.procMu.Unlock()
	if p.waitCh == nil {
		ch := make(chan error, 1)
		ch <- errors.New("extension already exited")
		return ch
	}
	return p.waitCh
}

func (p *extensionProcess) handleFailure() error {
	p.restartMu.Lock()
	defer p.restartMu.Unlock()
	p.procMu.Lock()
	hasProcess := p.cmd != nil
	p.procMu.Unlock()
	if !hasProcess {
		return nil
	}
	if err := p.stop(200 * time.Millisecond); err != nil {
		return err
	}
	p.procMu.Lock()
	p.cmd = nil
	p.stdin = nil
	p.encoder = nil
	p.stopOnce = sync.Once{}
	p.stopped = make(chan struct{})
	p.procMu.Unlock()
	return p.start()
}

func (p *extensionProcess) stop(timeout time.Duration) error {
	var result error
	p.stopOnce.Do(func() {
		p.procMu.Lock()
		cmd := p.cmd
		encoder := p.encoder
		waitCh := p.waitCh
		p.procMu.Unlock()
		if cmd == nil {
			close(p.stopped)
			return
		}
		if waitCh == nil {
			close(p.stopped)
			return
		}
		deadline := time.Now().Add(timeout)
		_ = encoder.Encode(Shutdown{Type: "shutdown", Deadline: deadline})
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-waitCh:
		case <-timer.C:
			_ = cmd.Process.Kill()
			<-waitCh
			result = errors.New("extension shutdown timed out")
		}
		close(p.stopped)
	})
	return result
}

func makeEvent(taskID, extensionName, version string, generation int, session, method, status string, startedAt time.Time, err error) TaskEvent {
	message := ""
	if err != nil {
		message = err.Error()
	}
	return TaskEvent{
		Time:       startedAt,
		TaskID:     taskID,
		Extension:  extensionName,
		Version:    version,
		Generation: generation,
		SessionID:  session,
		Method:     method,
		Status:     status,
		DurationMS: time.Since(startedAt).Milliseconds(),
		Error:      message,
	}
}

func capabilityForMethod(method string) string {
	switch method {
	case "echo", "sleep":
		return method
	case "add", "verify":
		if method == "add" {
			return "math"
		}
		return "verify"
	default:
		return ""
	}
}

func newToken(prefix string) string {
	var raw [16]byte
	_, _ = rand.Read(raw[:])
	return prefix + "_" + hex.EncodeToString(raw[:])
}

func errString(err error) string {
	if err == nil {
		return "extension exited"
	}
	return err.Error()
}
