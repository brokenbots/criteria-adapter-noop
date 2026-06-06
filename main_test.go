package main

import (
	"context"
	"testing"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
)

// captureSink collects events emitted by Execute.
type captureSink struct{ events []*v2.ExecuteEvent }

func (c *captureSink) Send(e *v2.ExecuteEvent) error {
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
