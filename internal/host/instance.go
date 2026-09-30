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

// ErrRetired is returned when an old-generation process is killed at
// the handover drain boundary before answering a still-pending task.
var ErrRetired = errors.New("extension retired before task completed")

// ErrShutdown is returned when the host shuts down and the grace
// boundary is reached before a pending task is answered.
var ErrShutdown = errors.New("extension terminated by host shutdown")

type pendingCall struct {
	ch chan *protocol.Message
}

// Instance is one running extension process and its message channel.
type Instance struct {
	name    string
	session string
	extVer  string
	caps    map[string]int

	cmd     *exec.Cmd
	stdin   io.WriteCloser
	writeMu sync.Mutex
	done    chan struct{} // closed when the process exits
	bye     chan struct{} // closed when a session-valid bye arrives

	mu          sync.Mutex
	pending     map[string]*pendingCall
	alive       bool
	terminating bool
	exitOK      bool
}

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
		bye:     make(chan struct{}),
		pending: map[string]*pendingCall{},
		alive:   true,
	}

	hello := &protocol.Message{
		Type:     protocol.TypeHello,
		Protocol: protocol.Version,
		Session:  inst.session,
	}
	if err := inst.write(hello); err != nil {
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
		err := cmd.Wait()
		inst.mu.Lock()
		inst.exitOK = err == nil
		inst.mu.Unlock()
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

// failAll fails every pending call with the given reason. It is used
// both for unexpected crashes and for explicit termination at an
// agreed drain boundary, so every blocked waiter always wakes with a
// concrete cause.
func (inst *Instance) failAll(err error) {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	inst.alive = false
	for id, pc := range inst.pending {
		delete(inst.pending, id)
		pc.ch <- &protocol.Message{Type: protocol.TypeError, Err: err.Error()}
	}
}

func (inst *Instance) write(msg *protocol.Message) error {
	inst.writeMu.Lock()
	defer inst.writeMu.Unlock()
	return protocol.Encode(inst.stdin, msg)
}

func (inst *Instance) isAlive() bool {
	inst.mu.Lock()
	defer inst.mu.Unlock()
	return inst.alive
}

// call sends one task and waits for the reply or ctx timeout.
func (inst *Instance) call(id, capability string, payload json.RawMessage, timeout time.Duration) (*protocol.Message, error) {
	inst.mu.Lock()
	if !inst.alive {
		inst.mu.Unlock()
		return nil, ErrCrashed
	}
	if inst.terminating {
		// The process is in handover/host-shutdown drain. Refuse rather
		// than queue behind the shutdown message, so the task is never
		// dispatched to two generations.
		inst.mu.Unlock()
		return nil, ErrRetired
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
	inst.writeMu.Lock()
	inst.mu.Lock()
	if !inst.alive {
		delete(inst.pending, id)
		inst.mu.Unlock()
		inst.writeMu.Unlock()
		return nil, ErrCrashed
	}
	if inst.terminating {
		// terminate() holds writeMu while sending shutdown, so reaching
		// here with writeMu held means the shutdown message has not yet
		// been written: refuse the task instead of racing it.
		delete(inst.pending, id)
		inst.mu.Unlock()
		inst.writeMu.Unlock()
		return nil, ErrRetired
	}
	if err := protocol.Encode(inst.stdin, msg); err != nil {
		delete(inst.pending, id)
		inst.mu.Unlock()
		inst.writeMu.Unlock()
		return nil, fmt.Errorf("send task: %w", err)
	}
	inst.mu.Unlock()
	inst.writeMu.Unlock()

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

// shutdown asks the extension to stop accepting work and exit, then
// escalates to SIGKILL after the grace period. It is used for
// startup rollback where the drain reason is not reported.
func (inst *Instance) shutdown(grace time.Duration) {
	_ = inst.terminate(grace, nil)
}

// shutdownNow kills a freshly spawned, never-published instance during
// rollback. It never delivered tasks, so there is nothing to drain.
func (inst *Instance) shutdownNow() {
	inst.mu.Lock()
	alive := inst.alive
	inst.mu.Unlock()
	if !alive {
		return
	}
	_ = inst.cmd.Process.Kill()
	<-inst.done
}

// terminate asks the process to stop accepting new work, lets it finish
// the tasks it already accepted, and waits for its bye. The returned
// reason is always explicit:
//
//   - "already_exited": the process was gone before termination;
//   - "drained": the process finished accepted work and sent bye;
//   - "exited": the process vanished without sending bye;
//   - "grace_killed": the grace boundary elapsed and it was SIGKILLed.
//
// killReason is handed to pending callers when the grace boundary is
// reached (ErrRetired for handover, ErrShutdown for host shutdown).
func (inst *Instance) terminate(grace time.Duration, killReason error) string {
	inst.mu.Lock()
	alive := inst.alive
	inst.terminating = true
	inst.mu.Unlock()
	if !alive {
		return "already_exited"
	}

	inst.writeMu.Lock()
	_ = protocol.Encode(inst.stdin, &protocol.Message{
		Type:    protocol.TypeShutdown,
		Session: inst.session,
	})
	inst.writeMu.Unlock()
	timer := time.NewTimer(grace)
	defer timer.Stop()
	select {
	case <-inst.bye:
		// bye is sent only after accepted tasks have been drained.
		<-inst.done
		return "drained"
	case <-timer.C:
		inst.failAll(killReason)
		_ = inst.cmd.Process.Kill()
		<-inst.done
		return "grace_killed"
	case <-inst.done:
		// cmd.Wait closes the stdout pipe, so a buffered bye can race
		// process reaping. The SDK contract is: it finishes accepted
		// tasks, sends bye, and exits 0. A zero exit therefore proves a
		// clean drain even if the bye line was lost to pipe teardown;
		// a non-zero exit means the process vanished without draining.
		inst.mu.Lock()
		ok := inst.exitOK
		inst.mu.Unlock()
		if ok {
			return "drained"
		}
		return "exited"
	}
}
