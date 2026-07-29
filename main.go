// Command criteria-adapter-noop is the standalone out-of-process noop adapter
// binary. It serves the protocol-v2 noop adapter via the public Go SDK: it
// opens sessions, optionally sleeps for delay_ms, and reports success. It runs
// no user code and exists for control-flow and pipeline scaffolding (and as the
// reference adapter for exercising the signing/lock path).
package main

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// Version is the adapter's protocol-v2 version string.
const Version = "2.0.0"

type noopService struct {
	adapterhost.UnimplementedPermissions
	mu       sync.Mutex
	sessions map[string]struct{}
}

func (s *noopService) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{
		Name:               "noop",
		Version:            Version,
		SourceUrl:          "https://github.com/brokenbots/criteria-adapter-noop",
		SdkProtocolVersion: "2",
		Platforms:          []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"},
		Capabilities:       []string{"parallel_safe"},
	}, nil
}

func (s *noopService) OpenSession(_ context.Context, request *v2.OpenSessionRequest) (*v2.OpenSessionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sessions == nil {
		s.sessions = map[string]struct{}{}
	}
	s.sessions[request.GetSessionId()] = struct{}{}
	return &v2.OpenSessionResponse{}, nil
}

func (s *noopService) Execute(ctx context.Context, request *v2.ExecuteRequest, sink adapterhost.ExecuteEventSender) error {
	s.mu.Lock()
	_, ok := s.sessions[request.GetSessionId()]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session %q", request.GetSessionId())
	}
	if rawDelay := request.GetInput()["delay_ms"]; rawDelay != "" {
		delayMS, err := strconv.Atoi(rawDelay)
		if err != nil || delayMS < 0 {
			return fmt.Errorf("invalid delay_ms %q", rawDelay)
		}
		if delayMS > 0 {
			timer := time.NewTimer(time.Duration(delayMS) * time.Millisecond)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return sink.Send(&v2.ExecuteEvent{
		Event: &v2.ExecuteEvent_Result{Result: &v2.ExecuteResult{Outcome: "success"}},
	})
}

func (s *noopService) Log(ctx context.Context, _ *v2.LogRequest, _ adapterhost.LogEventSender) error {
	// Hold the stream open for the lifetime of the session. The adapter host
	// runs the heartbeat ticker only while Log is executing; returning early
	// would cancel it before a single heartbeat is sent, causing the host to
	// declare the session crashed after its 90s stall threshold.
	<-ctx.Done()
	return nil
}

func (s *noopService) CloseSession(_ context.Context, request *v2.CloseSessionRequest) (*v2.CloseSessionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, request.GetSessionId())
	return &v2.CloseSessionResponse{}, nil
}

func main() {
	adapterhost.Serve(&noopService{sessions: map[string]struct{}{}})
}
