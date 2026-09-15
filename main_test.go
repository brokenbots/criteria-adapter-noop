package main

import (
	"context"
	"testing"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// captureSink collects events emitted by Execute.
type captureSink struct{ events []*v2.ExecuteEvent }

func (c *captureSink) Send(e *v2.ExecuteEvent) error {
	c.events = append(c.events, e)
	return nil
}

// captureLogSender collects events emitted by Log.
type captureLogSender struct{ events []*v2.LogEvent }

func (c *captureLogSender) Send(e *v2.LogEvent) error {
	c.events = append(c.events, e)
	return nil
}

func TestInfo(t *testing.T) {
	resp, err := (&noopService{}).Info(context.Background(), &v2.InfoRequest{})
	if err != nil {
		t.Fatalf("Info: %v", err)
	}
	if resp.GetName() != "noop" {
		t.Errorf("name = %q, want noop", resp.GetName())
	}
	if resp.GetSourceUrl() == "" {
		t.Error("source_url must be set for publishing")
	}
	if len(resp.GetPlatforms()) == 0 {
		t.Error("platforms must be set for a multi-arch publish")
	}

	// The capabilities gate which host features the adapter may use: the
	// tool-call caller mode requires adapter_tools on top of parallel_safe
	// (CRI-159).
	caps := map[string]bool{}
	for _, c := range resp.GetCapabilities() {
		caps[c] = true
	}
	if !caps["parallel_safe"] {
		t.Error("capabilities must include parallel_safe")
	}
	if !caps[adapterhost.CapabilityAdapterTools] {
		t.Error("capabilities must include adapter_tools for the tool-call caller mode")
	}
}

func TestExecuteSuccess(t *testing.T) {
	s := &noopService{sessions: map[string]struct{}{}}
	ctx := context.Background()
	const sid = "s1"
	if _, err := s.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: sid}); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	sink := &captureSink{}
	if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: sid, Input: map[string]string{"delay_ms": "0"}}, sink); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(sink.events) != 1 {
		t.Fatalf("got %d events, want 1", len(sink.events))
	}
	if got := sink.events[0].GetResult().GetOutcome(); got != "success" {
		t.Errorf("outcome = %q, want success", got)
	}

	if _, err := s.CloseSession(ctx, &v2.CloseSessionRequest{SessionId: sid}); err != nil {
		t.Fatalf("CloseSession: %v", err)
	}
}

func TestExecuteUnknownSession(t *testing.T) {
	s := &noopService{sessions: map[string]struct{}{}}
	err := s.Execute(context.Background(), &v2.ExecuteRequest{SessionId: "missing"}, &captureSink{})
	if err == nil {
		t.Fatal("expected error for unknown session")
	}
}

func TestExecuteInvalidDelay(t *testing.T) {
	s := &noopService{sessions: map[string]struct{}{}}
	ctx := context.Background()
	if _, err := s.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: "s"}); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}
	if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: "s", Input: map[string]string{"delay_ms": "-1"}}, &captureSink{}); err == nil {
		t.Fatal("expected error for negative delay_ms")
	}
}

// TestLogReturnsPromptly pins the SDK-owned log-stream contract: the adapter's
// Log returns immediately and never emits log events; the SDK keeps the
// stream and its heartbeat alive for the full session lifetime.
func TestLogReturnsPromptly(t *testing.T) {
	s := &noopService{sessions: map[string]struct{}{}}
	const sid = "s1"
	ctx := context.Background()
	if _, err := s.OpenSession(ctx, &v2.OpenSessionRequest{SessionId: sid}); err != nil {
		t.Fatalf("OpenSession: %v", err)
	}

	sender := &captureLogSender{}
	if err := s.Log(ctx, &v2.LogRequest{SessionId: sid}, sender); err != nil {
		t.Fatalf("Log: %v", err)
	}
	if len(sender.events) != 0 {
		t.Errorf("Log emitted %d events, want 0 (the SDK owns heartbeats)", len(sender.events))
	}
}
