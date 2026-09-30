package host

import (
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

	"extsys/internal/protocol"
)

// ErrCrashed is returned to pending callers when the extension process
// dies before answering them.
var ErrCrashed = errors.New("extension process exited")

type pendingCall struct {
	ch chan *protocol.Message
}

// Instance is one running extension process and its message channel.
type Instance struct {
	name    string
	session string
	extVer  string
	caps    map[string]int

	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{} // closed when the process exits

	mu      sync.Mutex
	pending map[string]*pendingCall
	alive   bool

	bye chan struct{} // closed when the extension sends bye
}

// EndReason explains why a wait on an instance finished.
type EndReason string

const (
	EndExited     EndReason = "exited"      // process exited of its own accord
	EndBye        EndReason = "bye"         // drained and sent bye
	EndGraceKill  EndReason = "grace_kill"  // exceeded grace/drain boundary
	EndHostClose  EndReason = "host_close"  // host shutdown interrupted the wait
)

func newSessionToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// spawn starts the child process and performs the hello handshake.
// The negotiated session token is fresh for every process start, so a
// restarted process can never resume or impersonate a previous session.
func spawn(mf Manifest, handshakeTimeout time.Duration) (*Instance, error) {
	cmd := exec.Command(mf.Path, mf.Args...)
	if len(mf.Env) > 0 {
		cmd.Env = append(os.Environ(), mf.Env...)
	}
	cmd.Stderr = os.Stderr // extension diagnostics stay visible
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start %s: %w", mf.Path, err)
	}

	inst := &Instance{
		session: newSessionToken(),
		cmd:     cmd,
		stdin:   stdin,
		done:    make(chan struct{}),
		pending: map[string]*pendingCall{},
		alive:   true,
		bye:     make(chan struct{}),
	}

	hello := &protocol.Message{
		Type:     protocol.TypeHello,
		Protocol: protocol.Version,
		Session:  inst.session,
	}
	if err := protocol.Encode(stdin, hello); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("send hello: %w", err)
	}

	ackCh := make(chan *protocol.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		dec := json.NewDecoder(stdout)
		var ack protocol.Message
		if err := dec.Decode(&ack); err != nil {
			errCh <- err
			return
		}
		ackCh <- &ack
	}()

	select {
	case ack := <-ackCh:
		if ack.Type != protocol.TypeHelloAck {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("expected hello_ack, got %q", ack.Type)
		}
		if !protocol.Compatible(ack.Protocol) {
			_ = cmd.Process.Kill()
			return nil, fmt.Errorf("incompatible protocol version %d", ack.Protocol)
		}
		if ack.Session != inst.session {
			_ = cmd.Process.Kill()
			return nil, errors.New("session token mismatch in hello_ack")
		}
		inst.name = ack.Name
		inst.extVer = ack.ExtVer
		inst.caps = map[string]int{}
		for _, c := range ack.Caps {
			inst.caps[c.Name] = c.Version
		}
	case err := <-errCh:
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("read hello_ack: %w", err)
	case <-time.After(handshakeTimeout):
		_ = cmd.Process.Kill()
		return nil, errors.New("handshake timed out")
	}

	go inst.serve(stdout)
	go func() {
		_ = cmd.Wait()
		inst.failAll(ErrCrashed)
		close(inst.done)
	}()
	return inst, nil
}

// serve reads results from the extension and routes them to callers.
func (inst *Instance) serve(stdout io.Reader) {
	dec := json.NewDecoder(stdout)
	for {
		var msg protocol.Message
		if err := dec.Decode(&msg); err != nil {
			return // process gone; cmd.Wait handles cleanup
		}
		// Drop messages from any stale/foreign session: a restarted
		// process must not be able to answer for its predecessor.
		if msg.Session != inst.session {
			continue
		}
		if msg.Type == protocol.TypeBye {
			select {
			case <-inst.bye:
			default:
				close(inst.bye)
			}
			continue
		}
		inst.mu.Lock()
		pc, ok := inst.pending[msg.ID]
		if ok {
			delete(inst.pending, msg.ID)
		}
		inst.mu.Unlock()
		if ok {
			pc.ch <- &msg
		}
	}
}

func (inst *Instance) failAll(err error) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.alive = false
	for id, pc := range inst.pending {
		delete(inst.pending, id)
		pc.ch <- &protocol.Message{Type: protocol.TypeError, Err: err.Error()}
	}
}

// call sends one task and waits for the reply or ctx timeout.
func (inst *Instance) call(id, capability string, payload json.RawMessage, timeout time.Duration) (*protocol.Message, error) {
	inst.mu.Lock()
	if !inst.alive {
		inst.mu.Unlock()
		return nil, ErrCrashed
	}
	pc := &pendingCall{ch: make(chan *protocol.Message, 1)}
	inst.pending[id] = pc
	inst.mu.Unlock()

	msg := &protocol.Message{
		Type:    protocol.TypeTask,
		Session: inst.session,
		ID:      id,
		Cap:     capability,
		Payload: payload,
	}
	if err := protocol.Encode(inst.stdin, msg); err != nil {
		inst.mu.Lock()
		delete(inst.pending, id)
		inst.mu.Unlock()
		return nil, fmt.Errorf("send task: %w", err)
	}

	select {
	case resp := <-pc.ch:
		return resp, nil
	case <-time.After(timeout):
		inst.mu.Lock()
		delete(inst.pending, id)
		inst.mu.Unlock()
		return nil, fmt.Errorf("task timed out after %s", timeout)
	}
}

// inFlight reports how many tasks have been dispatched but not answered.
func (inst *Instance) inFlight() int {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return len(inst.pending)
}

// waitExit blocks until the process exits, the deadline elapses, or
// stop closes. It always returns an explicit reason for why the wait
// ended; deadline<=0 means "no drain boundary". When killed, pending
// callers are failed by the cmd.Wait goroutine.
func (inst *Instance) waitExit(deadline <-chan time.Time, stop <-chan struct{}) EndReason {
	select {
	case <-inst.done:
		return EndExited
	default:
	}
	select {
	case <-inst.done:
		return EndExited
	case <-inst.bye:
		select {
		case <-inst.done:
			return EndBye
		case <-deadlineOrNil(deadline):
			_ = inst.cmd.Process.Kill()
			<-inst.done
			return EndGraceKill
		case <-stopOrNil(stop):
			_ = inst.cmd.Process.Kill()
			<-inst.done
			return EndHostClose
		}
	case <-deadlineOrNil(deadline):
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return EndGraceKill
	case <-stopOrNil(stop):
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return EndHostClose
	}
}

func deadlineOrNil(c <-chan time.Time) <-chan time.Time {
	if c == nil {
		return nil
	}
	return c
}

func stopOrNil(c <-chan struct{}) <-chan struct{} {
	if c == nil {
		return nil
	}
	return c
}

// signalShutdown sends the shutdown directive when the instance is
// still alive.
func (inst *Instance) signalShutdown() bool {
	inst.mu.Lock()
	alive := inst.alive
	inst.mu.Unlock()
	if !alive {
		return false
	}
	_ = protocol.Encode(inst.stdin, &protocol.Message{
		Type:    protocol.TypeShutdown,
		Session: inst.session,
	})
	return true
}

// drain asks the old instance to stop accepting new work, then waits
// for in-flight tasks to finish and the process to exit. The grace
// duration is the agreed drain boundary; beyond it the process is
// killed. stop interrupts the wait with an explicit host-close reason.
func (inst *Instance) drain(grace time.Duration, stop <-chan struct{}) EndReason {
	if !inst.signalShutdown() {
		return EndExited
	}
	var deadline <-chan time.Time
	if grace > 0 {
		t := time.NewTimer(grace)
		defer t.Stop()
		deadline = t.C
	}
	return inst.waitExit(deadline, stop)
}

// shutdown asks the extension to stop accepting work and exit, then
// escalates to SIGKILL after the grace period.
func (inst *Instance) shutdown(grace time.Duration) {
	inst.drain(grace, nil)
}

// killNow terminates the process immediately and waits for cleanup.
func (inst *Instance) killNow() {
	inst.mu.Lock()
	alive := inst.alive
	inst.mu.Unlock()
	if !alive {
		return
	}
	_ = inst.cmd.Process.Kill()
	<-inst.done
}
