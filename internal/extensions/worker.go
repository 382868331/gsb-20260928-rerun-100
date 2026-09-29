package extensions

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

func builtinManifest(builtin string) (Manifest, error) {
	switch builtin {
	case "echo":
		return Manifest{
			Name:         "echo",
			Version:      "1.2.0",
			Builtin:      "echo",
			Capabilities: []string{"echo", "delay", "test-faults"},
		}, nil
	case "calculator":
		return Manifest{
			Name:         "calculator",
			Version:      "2.0.1",
			Builtin:      "calculator",
			Capabilities: []string{"math", "verify", "test-faults"},
			Requires: []Requirement{{
				Extension:  "echo",
				MinVersion: "1.0.0",
				Capability: "echo",
			}},
		}, nil
	default:
		return Manifest{}, fmt.Errorf("unknown builtin extension %q", builtin)
	}
}

func BuiltinManifests() []Manifest {
	return []Manifest{
		mustManifest("echo"),
		mustManifest("calculator"),
	}
}

func mustManifest(name string) Manifest {
	m, err := builtinManifest(name)
	if err != nil {
		panic(err)
	}
	return m
}

func RunWorker(builtin string, in io.Reader, out io.Writer) error {
	manifest, err := builtinManifest(builtin)
	if err != nil {
		return err
	}

	decoder := json.NewDecoder(bufio.NewReader(in))
	encoder := json.NewEncoder(out)
	var init Init
	if err := decoder.Decode(&init); err != nil {
		return err
	}
	if init.Type != "init" || init.Protocol != ProtocolVersion {
		return fmt.Errorf("incompatible handshake protocol %d", init.Protocol)
	}
	if err := encoder.Encode(Hello{
		Type:         "hello",
		Protocol:     ProtocolVersion,
		Name:         manifest.Name,
		Version:      manifest.Version,
		Capabilities: manifest.Capabilities,
	}); err != nil {
		return err
	}
	if err := encoder.Encode(Ready{Type: "ready", SessionID: init.SessionID}); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	active := map[string]context.CancelFunc{}
	results := make(chan Response, 16)
	messages := make(chan json.RawMessage, 16)
	decoderDone := make(chan error, 1)
	go func() {
		for {
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err != nil {
				decoderDone <- err
				return
			}
			messages <- raw
		}
	}()

	for {
		select {
		case raw := <-messages:
			var head struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal(raw, &head); err != nil {
				continue
			}
			switch head.Type {
			case "request":
				var req Request
				if err := json.Unmarshal(raw, &req); err != nil {
					continue
				}
				reqCtx, reqCancel := context.WithCancel(ctx)
				active[req.ID] = reqCancel
				go func(req Request) {
					result, err := handleRequest(manifest, reqCtx, req)
					if err != nil {
						code := "internal_error"
						if errors.Is(err, context.DeadlineExceeded) {
							code = "deadline_exceeded"
						}
						results <- Response{Type: "response", ID: req.ID, SessionID: init.SessionID, OK: false, ErrorCode: code, Error: err.Error()}
						return
					}
					results <- Response{Type: "response", ID: req.ID, SessionID: init.SessionID, OK: true, Result: result}
				}(req)
			case "cancel":
				var msg Cancel
				if err := json.Unmarshal(raw, &msg); err == nil {
					if stop := active[msg.ID]; stop != nil {
						stop()
					}
				}
			case "shutdown":
				cancel()
				return nil
			}
		case response := <-results:
			if stop := active[response.ID]; stop != nil {
				stop()
				delete(active, response.ID)
			}
			if err := encoder.Encode(response); err != nil {
				return err
			}
		case err := <-decoderDone:
			cancel()
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func handleRequest(manifest Manifest, ctx context.Context, req Request) (string, error) {
	deadline := time.Time{}
	if !req.Deadline.IsZero() {
		deadline = req.Deadline
		var cancel context.CancelFunc
		ctx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	switch manifest.Builtin {
	case "echo":
		switch req.Method {
		case "echo":
			return req.Params, nil
		case "sleep":
			ms, err := strconv.Atoi(strings.TrimSpace(req.Params))
			if err != nil {
				return "", fmt.Errorf("sleep requires milliseconds: %w", err)
			}
			select {
			case <-time.After(time.Duration(ms) * time.Millisecond):
				return "slept", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		case "crash":
			os.Exit(77)
		}
	case "calculator":
		switch req.Method {
		case "add":
			parts := strings.Split(req.Params, ",")
			total := 0
			for _, part := range parts {
				value, err := strconv.Atoi(strings.TrimSpace(part))
				if err != nil {
					return "", fmt.Errorf("add requires integers: %w", err)
				}
				total += value
			}
			return strconv.Itoa(total), nil
		case "verify":
			return "ok", nil
		case "crash":
			os.Exit(77)
		}
	}
	return "", fmt.Errorf("unknown method %q", req.Method)
}
