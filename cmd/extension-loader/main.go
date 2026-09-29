package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"extension-loader/internal/extensions"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "__extension-worker" {
		if err := extensions.RunWorker(os.Args[2], os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	auditPath := flag.String("audit", "extension-events.jsonl", "JSONL task audit path")
	flag.Parse()
	if err := run(*auditPath); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(auditPath string) error {
	audit, err := extensions.NewJSONAuditor(auditPath)
	if err != nil {
		return err
	}
	defer audit.Close()

	host := extensions.NewHost(audit)
	if err := host.Start(); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tasks := []struct {
		name    string
		method  string
		params  string
		timeout time.Duration
	}{
		{name: "echo", method: "echo", params: "hello from builtin extension", timeout: 2 * time.Second},
		{name: "calculator", method: "add", params: "7,8,9", timeout: 2 * time.Second},
		{name: "echo", method: "sleep", params: "50", timeout: 2 * time.Second},
	}

	var result string
	var event extensions.TaskEvent
	for _, task := range tasks {
		result, event, err = host.Call(ctx, task.name, task.method, task.params, task.timeout)
		if err != nil {
			return fmt.Errorf("task %s/%s: %w", task.name, task.method, err)
		}
		fmt.Printf("%s.%s => %s (v%s, generation %d)\n", task.name, task.method, result, event.Version, event.Generation)
	}

	_, event, err = host.Call(ctx, "echo", "sleep", "300", 75*time.Millisecond)
	if err == nil || event.Status != "timed_out" {
		return fmt.Errorf("expected timeout to be isolated: %w", err)
	}
	fmt.Printf("echo.sleep timed out after %dms; the request failed without taking down the host\n", event.DurationMS)

	if _, _, err := host.Call(ctx, "echo", "echo", "still accepting work", time.Second); err != nil {
		return fmt.Errorf("host unavailable after timeout: %w", err)
	}

	_, event, err = host.Call(ctx, "echo", "crash", "", time.Second)
	if err == nil || event.Status != "failed" {
		return fmt.Errorf("expected crash to fail only its request: %w", err)
	}
	fmt.Printf("echo.crash ended generation %d session %s; restarting with a new session\n", event.Generation, event.SessionID)

	result, event, err = host.Call(ctx, "echo", "echo", "new process, new session", time.Second)
	if err != nil {
		return fmt.Errorf("host unavailable after crash: %w", err)
	}
	if event.Generation != 2 {
		return fmt.Errorf("expected restarted generation 2, got %d", event.Generation)
	}
	fmt.Printf("echo.echo => %s (v%s, generation %d, session %s)\n", result, event.Version, event.Generation, event.SessionID)

	if err := host.Close(2 * time.Second); err != nil {
		return err
	}
	fmt.Println("all extensions stopped")
	return nil
}
