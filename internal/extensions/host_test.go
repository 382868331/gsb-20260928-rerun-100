package extensions_test

import (
	"context"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"extension-loader/internal/extensions"
)

type memoryAuditor struct {
	events []extensions.TaskEvent
}

func (a *memoryAuditor) Record(event extensions.TaskEvent) {
	a.events = append(a.events, event)
}

func buildLoader(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "loader")
	cmd := exec.Command("go", "build", "-o", binary, "../../cmd/extension-loader")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build loader: %v\n%s", err, output)
	}
	return binary
}

func TestHostTasksTimeoutCrashRestartAndAudit(t *testing.T) {
	auditor := &memoryAuditor{}
	host := extensions.NewHostWithExecutable(buildLoader(t), auditor)
	if err := host.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = host.Close(2 * time.Second) })

	echo, event, err := host.Call(context.Background(), "echo", "echo", "ping", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if echo != "ping" || event.Version != "1.2.0" || event.Generation != 1 {
		t.Fatalf("unexpected echo result/event: %q %+v", echo, event)
	}

	sum, event, err := host.Call(context.Background(), "calculator", "add", "2,3,4", time.Second)
	if err != nil || sum != "9" || event.Version != "2.0.1" {
		t.Fatalf("unexpected calculator result: %q %+v %v", sum, event, err)
	}

	_, timeoutEvent, err := host.Call(context.Background(), "echo", "sleep", "300", 50*time.Millisecond)
	if err == nil || timeoutEvent.Status != "timed_out" {
		t.Fatalf("expected timeout, got event %+v err %v", timeoutEvent, err)
	}

	afterTimeout, _, err := host.Call(context.Background(), "echo", "echo", "alive", time.Second)
	if err != nil {
		t.Fatalf("host not usable after timeout: %v", err)
	}
	if afterTimeout != "alive" {
		t.Fatalf("unexpected response after timeout: %q", afterTimeout)
	}

	_, crashEvent, err := host.Call(context.Background(), "echo", "crash", "", time.Second)
	if err == nil || crashEvent.Status != "failed" {
		t.Fatalf("expected crash failure, got event %+v err %v", crashEvent, err)
	}

	recovered, recoveryEvent, err := host.Call(context.Background(), "echo", "echo", "restarted", time.Second)
	if err != nil {
		t.Fatalf("host did not recover crashed extension: %v", err)
	}
	if recovered != "restarted" || recoveryEvent.Generation != 2 {
		t.Fatalf("unexpected recovered result/event: %q %+v", recovered, recoveryEvent)
	}
	if recoveryEvent.SessionID == crashEvent.SessionID {
		t.Fatal("restarted extension reused the original session id")
	}

	if len(auditor.events) < 6 {
		t.Fatalf("expected audit records for every dispatch, got %d", len(auditor.events))
	}
	for _, recorded := range auditor.events {
		if recorded.Extension == "" || recorded.Version == "" || recorded.SessionID == "" {
			t.Fatalf("audit event cannot identify version/session: %+v", recorded)
		}
	}
}
