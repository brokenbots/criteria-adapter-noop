package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// toolTestHost is the fake adapter host for the tool-call caller tests. It
// mirrors the SDK's toolcall_test.go fake host: in-memory channels between the
// adapter under test and a scripted host goroutine, with the host playing the
// engine's CRI-159/160 seam (allow-grant, nested callee execution, and the
// correlated ToolCallResult reply).
type toolTestHost struct {
	t *testing.T

	// handle is the test's scripted reply: given the request_id and payload of
	// a permission.request AdapterEvent, it returns the PermissionEvents the
	// host emits on the Permissions stream.
	handle func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent

	executeC  chan *v2.ExecuteEvent
	permC     chan *v2.PermissionEvent
	stopC     chan struct{}
	loopDone  chan struct{}
	permDone  chan struct{}
	ctx       context.Context
	cancelCtx context.CancelFunc
	// resultRecorded is signalled (non-blocking) every time the host records
	// a result event, so tests can synchronize with the host goroutine.
	resultRecorded chan struct{}

	mu      sync.Mutex
	sent    []*structpb.Struct
	results []*v2.ExecuteEvent
	acks    []*v2.PermissionDecision
}

func newToolTestHost(t *testing.T) *toolTestHost {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &toolTestHost{
		t:              t,
		executeC:       make(chan *v2.ExecuteEvent, 16),
		permC:          make(chan *v2.PermissionEvent, 32),
		stopC:          make(chan struct{}),
		loopDone:       make(chan struct{}),
		permDone:       make(chan struct{}),
		ctx:            ctx,
		cancelCtx:      cancel,
		resultRecorded: make(chan struct{}, 1),
	}
}

// start launches the fake host: one goroutine consuming the adapter's Execute
// stream (recording results and dispatching permission.request payloads) and
// one running the service's Permissions dispatch loop.
func (h *toolTestHost) start(svc *noopService) {
	go func() {
		defer close(h.loopDone)
		for {
			select {
			case ev := <-h.executeC:
				if ev.GetResult() != nil || ev.GetHeartbeat() != nil {
					// The adapter's own ExecuteResult, not a wire request.
					h.mu.Lock()
					h.results = append(h.results, ev)
					h.mu.Unlock()
					select {
					case h.resultRecorded <- struct{}{}:
					default:
					}
					continue
				}
				payload := ev.GetAdapter().GetPayload()
				fields := payload.GetFields()
				requestID := fields["request_id"].GetStringValue()
				h.mu.Lock()
				h.sent = append(h.sent, payload)
				h.mu.Unlock()
				if h.handle != nil {
					for _, pe := range h.handle(requestID, payload) {
						select {
						case h.permC <- pe:
						case <-h.stopC:
							return
						}
					}
				}
			case <-h.stopC:
				return
			}
		}
	}()
	go func() {
		defer close(h.permDone)
		_ = svc.Permissions(h.ctx, hostPermissionsStream{h: h})
	}()
}

// stop shuts both host goroutines down and fails the test if they do not exit,
// so a hang in the adapter surfaces as a test failure rather than a leak.
func (h *toolTestHost) stop() {
	close(h.stopC)
	h.cancelCtx()
	for name, done := range map[string]<-chan struct{}{
		"execute loop":    h.loopDone,
		"permission loop": h.permDone,
	} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			h.t.Fatalf("fake host %s did not stop", name)
		}
	}
}

func (h *toolTestHost) sink() adapterhost.ExecuteEventSender { return hostExecuteSink{h: h} }

func (h *toolTestHost) sentPayloads() []*structpb.Struct {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*structpb.Struct(nil), h.sent...)
}

func (h *toolTestHost) resultEvents() []*v2.ExecuteEvent {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*v2.ExecuteEvent(nil), h.results...)
}

func (h *toolTestHost) acksSent() []*v2.PermissionDecision {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]*v2.PermissionDecision(nil), h.acks...)
}

type hostExecuteSink struct{ h *toolTestHost }

func (s hostExecuteSink) Send(ev *v2.ExecuteEvent) error {
	select {
	case s.h.executeC <- ev:
		return nil
	case <-time.After(5 * time.Second):
		return errors.New("test: fake host did not consume the execute event")
	}
}

type hostPermissionsStream struct{ h *toolTestHost }

func (s hostPermissionsStream) Recv() (*v2.PermissionEvent, error) {
	select {
	case ev, ok := <-s.h.permC:
		if !ok {
			return nil, io.EOF
		}
		return ev, nil
	case <-s.h.ctx.Done():
		return nil, s.h.ctx.Err()
	}
}

func (s hostPermissionsStream) Send(d *v2.PermissionDecision) error {
	s.h.mu.Lock()
	defer s.h.mu.Unlock()
	s.h.acks = append(s.h.acks, d)
	return nil
}

func (s hostPermissionsStream) Context() context.Context { return s.h.ctx }

func grantEvent(requestID string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Request{Request: &v2.PermissionRequest{RequestId: requestID}},
	}
}

func cancelEvent(requestID, reason string) *v2.PermissionEvent {
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_Cancel{Cancel: &v2.PermissionCancel{RequestId: requestID, Reason: reason}},
	}
}

func resultEvent(requestID string, res *v2.ToolCallResult) *v2.PermissionEvent {
	// The host echoes the caller's payload request_id into every reply.
	res.RequestId = requestID
	return &v2.PermissionEvent{
		Event: &v2.PermissionEvent_ToolCallResult{ToolCallResult: res},
	}
}

// grantAndCallCallee scripts the host handler mirroring the engine's
// CRI-159/160 seam: the allow-grant is emitted, the call args are rendered as
// the callee's wire input, the real callee Execute runs, and its outcome and
// outputs flow back as the ToolCallResult.
func grantAndCallCallee(callee *noopService, sessionID string) func(string, *structpb.Struct) []*v2.PermissionEvent {
	return func(requestID string, payload *structpb.Struct) []*v2.PermissionEvent {
		events := []*v2.PermissionEvent{grantEvent(requestID)}
		res, err := callCallee(callee, sessionID, payload)
		if err != nil {
			// A callee session failure surfaces as a typed call_error.
			return append(events, resultEvent(requestID, &v2.ToolCallResult{CallError: adapterhost.CallErrCalleeCrash}))
		}
		return append(events, resultEvent(requestID, res))
	}
}

// callCallee plays the engine's nested-execution step: it renders the call
// args as the callee's wire input (string values pass through raw, every other
// JSON type is encoded, mirroring the engine's calleeInputFromArgs), runs the
// callee's Execute, and returns its outcome and outputs.
func callCallee(callee *noopService, sessionID string, payload *structpb.Struct) (*v2.ToolCallResult, error) {
	args := payload.GetFields()["args"].GetStructValue().AsMap()
	input := make(map[string]string, len(args))
	for key, val := range args {
		if s, ok := val.(string); ok {
			input[key] = s
			continue
		}
		encoded, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		input[key] = string(encoded)
	}
	sink := &captureSink{}
	if err := callee.Execute(context.Background(), &v2.ExecuteRequest{SessionId: sessionID, Input: input}, sink); err != nil {
		return nil, err
	}
	if len(sink.events) != 1 {
		return nil, fmt.Errorf("test: callee emitted %d events, want 1", len(sink.events))
	}
	result := sink.events[0].GetResult()
	if result == nil {
		return nil, errors.New("test: callee emitted a non-result event")
	}
	return &v2.ToolCallResult{Outcome: result.GetOutcome(), OutputsJson: result.GetOutputsJson()}, nil
}

// openSessions opens every named session on the service.
func openSessions(t *testing.T, s *noopService, sessionIDs ...string) {
	t.Helper()
	for _, sid := range sessionIDs {
		if _, err := s.OpenSession(context.Background(), &v2.OpenSessionRequest{SessionId: sid}); err != nil {
			t.Fatalf("OpenSession(%q): %v", sid, err)
		}
	}
}

// callerResult waits for the host to record the caller's final ExecuteResult
// and returns the most recent one.
func callerResult(t *testing.T, h *toolTestHost) *v2.ExecuteResult {
	t.Helper()
	select {
	case <-h.resultRecorded:
	case <-time.After(5 * time.Second):
		h.t.Fatal("caller result event was never recorded")
	}
	events := h.resultEvents()
	if len(events) < 1 {
		t.Fatalf("caller emitted %d result events, want at least 1", len(events))
	}
	return events[len(events)-1].GetResult()
}

// requireResultCount asserts the host recorded exactly want result events.
func requireResultCount(t *testing.T, h *toolTestHost, want int) {
	t.Helper()
	if got := len(h.resultEvents()); got != want {
		t.Fatalf("caller emitted %d result events, want %d", got, want)
	}
}

// decodedOutputs decodes an ExecuteResult.outputs_json payload.
func decodedOutputs(t *testing.T, result *v2.ExecuteResult) map[string]any {
	t.Helper()
	var outputs map[string]any
	if err := json.Unmarshal(result.GetOutputsJson(), &outputs); err != nil {
		t.Fatalf("decode outputs_json %q: %v", result.GetOutputsJson(), err)
	}
	return outputs
}

// TestExecuteToolCallPassthrough drives the reference round trip end to end: a
// caller noop step tool-calls a callee noop step, the callee emits typed
// outputs, and those surface under callee.* on the caller side alongside the
// CRI-152 payload contract and the grant ACK.
func TestExecuteToolCallPassthrough(t *testing.T) {
	ctx := context.Background()
	caller := &noopService{sessions: map[string]struct{}{}}
	callee := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, caller, "caller")
	openSessions(t, callee, "callee")

	host := newToolTestHost(t)
	host.handle = grantAndCallCallee(callee, "callee")
	host.start(caller)
	defer host.stop()

	const calleeOutputs = `{"message":"hi","count":3,"nested":{"ok":true}}`
	err := caller.Execute(ctx, &v2.ExecuteRequest{
		SessionId: "caller",
		Input: map[string]string{
			"tool_target": "adapter.noop.callee.tools.emit",
			"tool_args":   `{"outputs":` + strconv.Quote(calleeOutputs) + `}`,
		},
	}, host.sink())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// The caller's outcome reflects the successful round-trip and the
	// callee's outcome and outputs pass through typed under callee.*.
	result := callerResult(t, host)
	requireResultCount(t, host, 1)
	if got := result.GetOutcome(); got != "success" {
		t.Errorf("caller outcome = %q, want success", got)
	}
	outputs := decodedOutputs(t, result)
	if got := outputs["callee.outcome"]; got != "success" {
		t.Errorf("callee.outcome = %v, want success", got)
	}
	wantOutputs := map[string]any{
		"message": "hi",
		"count":   float64(3),
		"nested":  map[string]any{"ok": true},
	}
	if diff := jsonDiff(outputs["callee.outputs"], wantOutputs); diff != "" {
		t.Errorf("callee.outputs mismatch: %s", diff)
	}
	if _, has := outputs["callee.error"]; has {
		t.Error("callee.error must be absent on a successful round-trip")
	}

	// CRI-152 payload contract on the wire.
	sent := host.sentPayloads()
	if len(sent) != 1 {
		t.Fatalf("sent %d permission.request payloads, want 1", len(sent))
	}
	fields := sent[0].GetFields()
	if got := fields["kind"].GetStringValue(); got != adapterhost.PayloadKindAdapterTool {
		t.Errorf("payload kind = %q, want %q", got, adapterhost.PayloadKindAdapterTool)
	}
	if fields["request_id"].GetStringValue() == "" {
		t.Error("payload request_id is empty; the host cannot correlate the reply")
	}
	if got := fields["target"].GetStringValue(); got != "adapter.noop.callee.tools.emit" {
		t.Errorf("payload target = %q, want adapter.noop.callee.tools.emit", got)
	}
	if got := fields["tool"].GetStringValue(); got != "emit" {
		t.Errorf("payload tool = %q, want emit", got)
	}
	wantArgs := map[string]any{"outputs": calleeOutputs}
	if diff := jsonDiff(fields["args"].GetStructValue().AsMap(), wantArgs); diff != "" {
		t.Errorf("payload args mismatch: %s", diff)
	}
	wantDigest, err := v2.ArgsDigest(wantArgs)
	if err != nil {
		t.Fatalf("ArgsDigest: %v", err)
	}
	if got := fields["args_digest"].GetStringValue(); got != wantDigest {
		t.Errorf("payload args_digest = %q, want %q", got, wantDigest)
	}

	// The allow-grant was ACKed so the host can retire the request.
	acks := host.acksSent()
	if len(acks) != 1 {
		t.Fatalf("sent %d permission decisions, want 1", len(acks))
	}
	if got := acks[0]; got.GetRequestId() != fields["request_id"].GetStringValue() || got.GetDecision() != "allow" {
		t.Errorf("ack = {request_id: %q, decision: %q}, want the grant ACKed with allow", got.GetRequestId(), got.GetDecision())
	}
}

// TestExecuteToolCallCalleeFailurePassthrough covers a callee that ran and
// reported a failure outcome: the caller's own outcome still reflects the
// successful round-trip, and the callee's negative result passes through.
func TestExecuteToolCallCalleeFailurePassthrough(t *testing.T) {
	ctx := context.Background()
	caller := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, caller, "caller")

	host := newToolTestHost(t)
	host.handle = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{
			grantEvent(requestID),
			resultEvent(requestID, &v2.ToolCallResult{
				Outcome:     "failure",
				OutputsJson: []byte(`{"skipped":true}`),
			}),
		}
	}
	host.start(caller)
	defer host.stop()

	err := caller.Execute(ctx, &v2.ExecuteRequest{
		SessionId: "caller",
		Input: map[string]string{
			"tool_target": "adapter.noop.callee.tools.emit",
		},
	}, host.sink())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	result := callerResult(t, host)
	if got := result.GetOutcome(); got != "success" {
		t.Errorf("caller outcome = %q, want success (the round-trip itself succeeded)", got)
	}
	outputs := decodedOutputs(t, result)
	if got := outputs["callee.outcome"]; got != "failure" {
		t.Errorf("callee.outcome = %v, want failure", got)
	}
	wantOutputs := map[string]any{"skipped": true}
	if diff := jsonDiff(outputs["callee.outputs"], wantOutputs); diff != "" {
		t.Errorf("callee.outputs mismatch: %s", diff)
	}
}

// TestExecuteToolCallRoundTripFailure covers a failed call round-trip: the
// caller's own outcome flips to failure with the reason under callee.error,
// instead of failing Execute.
func TestExecuteToolCallRoundTripFailure(t *testing.T) {
	ctx := context.Background()
	caller := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, caller, "caller")

	t.Run("call_error", func(t *testing.T) {
		host := newToolTestHost(t)
		host.handle = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
			return []*v2.PermissionEvent{
				grantEvent(requestID),
				resultEvent(requestID, &v2.ToolCallResult{CallError: adapterhost.CallErrUnknownAdapter}),
			}
		}
		host.start(caller)
		defer host.stop()

		if err := caller.Execute(ctx, &v2.ExecuteRequest{
			SessionId: "caller",
			Input:     map[string]string{"tool_target": "adapter.shell.missing.tools.git_status"},
		}, host.sink()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		result := callerResult(t, host)
		if got := result.GetOutcome(); got != "failure" {
			t.Errorf("caller outcome = %q, want failure", got)
		}
		outputs := decodedOutputs(t, result)
		if got, ok := outputs["callee.error"].(string); !ok || !strings.Contains(got, adapterhost.CallErrUnknownAdapter) {
			t.Errorf("callee.error = %v, want it to carry the call_error code", outputs["callee.error"])
		}
	})

	t.Run("denied", func(t *testing.T) {
		host := newToolTestHost(t)
		host.handle = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
			return []*v2.PermissionEvent{cancelEvent(requestID, "policy: shell tools disabled")}
		}
		host.start(caller)
		defer host.stop()

		if err := caller.Execute(ctx, &v2.ExecuteRequest{
			SessionId: "caller",
			Input:     map[string]string{"tool_target": "adapter.shell.worker.tools.git_status"},
		}, host.sink()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		result := callerResult(t, host)
		if got := result.GetOutcome(); got != "failure" {
			t.Errorf("caller outcome = %q, want failure", got)
		}
		outputs := decodedOutputs(t, result)
		got, _ := outputs["callee.error"].(string)
		if !strings.Contains(got, "denied") || !strings.Contains(got, "policy: shell tools disabled") {
			t.Errorf("callee.error = %q, want it to carry the denial reason", got)
		}
	})
}

// TestExecuteToolCallHostUnsupported covers the old-host signature: a bare
// allow-grant with no ToolCallResult within the call deadline resolves as
// host_unsupported, the session is cached, and a later call on the same
// session fails fast without sending.
func TestExecuteToolCallHostUnsupported(t *testing.T) {
	ctx := context.Background()
	caller := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, caller, "caller")

	host := newToolTestHost(t)
	host.handle = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
		return []*v2.PermissionEvent{grantEvent(requestID)}
	}
	host.start(caller)
	defer host.stop()

	callCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	if err := caller.Execute(callCtx, &v2.ExecuteRequest{
		SessionId: "caller",
		Input:     map[string]string{"tool_target": "adapter.noop.callee.tools.emit"},
	}, host.sink()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	result := callerResult(t, host)
	if got := result.GetOutcome(); got != "failure" {
		t.Errorf("caller outcome = %q, want failure", got)
	}
	outputs := decodedOutputs(t, result)
	if got, ok := outputs["callee.error"].(string); !ok || !strings.Contains(got, adapterhost.CallErrHostUnsupported) {
		t.Errorf("callee.error = %v, want it to carry host_unsupported", outputs["callee.error"])
	}

	// The second call on the same session fails fast without another wire
	// request.
	before := len(host.sentPayloads())
	start := time.Now()
	if err := caller.Execute(ctx, &v2.ExecuteRequest{
		SessionId: "caller",
		Input:     map[string]string{"tool_target": "adapter.noop.callee.tools.emit"},
	}, host.sink()); err != nil {
		t.Fatalf("Execute (fail-fast): %v", err)
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Errorf("fail-fast call took %v; want it to return without waiting for a deadline", elapsed)
	}
	if got := len(host.sentPayloads()); got != before {
		t.Errorf("fail-fast call sent %d new payloads; want 0", got-before)
	}
	result = callerResult(t, host)
	if got := result.GetOutcome(); got != "failure" {
		t.Errorf("fail-fast caller outcome = %q, want failure", got)
	}
	requireResultCount(t, host, 2)
}

// TestExecuteToolCallBareTargetAndToolName covers tool resolution for a bare
// surface target: tool_name names the tool and the wire target is normalized
// to the named §2 form. tool_args unset sends an empty args object.
func TestExecuteToolCallBareTargetAndToolName(t *testing.T) {
	ctx := context.Background()
	caller := &noopService{sessions: map[string]struct{}{}}
	callee := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, caller, "caller")
	openSessions(t, callee, "callee")

	host := newToolTestHost(t)
	host.handle = grantAndCallCallee(callee, "callee")
	host.start(caller)
	defer host.stop()

	err := caller.Execute(ctx, &v2.ExecuteRequest{
		SessionId: "caller",
		Input: map[string]string{
			"tool_target": "adapter.noop.callee.tools",
			"tool_name":   "emit",
		},
	}, host.sink())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	sent := host.sentPayloads()
	if len(sent) != 1 {
		t.Fatalf("sent %d permission.request payloads, want 1", len(sent))
	}
	fields := sent[0].GetFields()
	if got := fields["target"].GetStringValue(); got != "adapter.noop.callee.tools.emit" {
		t.Errorf("payload target = %q, want the bare target resolved with tool_name", got)
	}
	if got := fields["tool"].GetStringValue(); got != "emit" {
		t.Errorf("payload tool = %q, want emit", got)
	}
	if diff := jsonDiff(fields["args"].GetStructValue().AsMap(), map[string]any{}); diff != "" {
		t.Errorf("payload args mismatch: %s", diff)
	}
	wantDigest, err := v2.ArgsDigest(map[string]any{})
	if err != nil {
		t.Fatalf("ArgsDigest: %v", err)
	}
	if got := fields["args_digest"].GetStringValue(); got != wantDigest {
		t.Errorf("payload args_digest = %q, want %q", got, wantDigest)
	}

	// The callee ran in default mode with no outputs: the passthrough carries
	// the callee outcome and an empty outputs object.
	result := callerResult(t, host)
	if got := result.GetOutcome(); got != "success" {
		t.Fatalf("caller outcome = %q, want success", got)
	}
	outputs := decodedOutputs(t, result)
	if got := outputs["callee.outcome"]; got != "success" {
		t.Errorf("callee.outcome = %v, want success", got)
	}
	calleeOutputs, ok := outputs["callee.outputs"].(map[string]any)
	if !ok || len(calleeOutputs) != 0 {
		t.Errorf("callee.outputs = %v, want an empty object", outputs["callee.outputs"])
	}
}

// TestExecuteToolCallInvalidInput covers the malformed tool-call inputs: they
// fail Execute without any wire traffic.
func TestExecuteToolCallInvalidInput(t *testing.T) {
	cases := map[string]map[string]string{
		"not rooted at adapter":  {"tool_target": "noop.callee.tools.emit"},
		"too few labels":         {"tool_target": "adapter.noop.callee"},
		"too many labels":        {"tool_target": "adapter.noop.callee.tools.emit.extra"},
		"non-bareword label":     {"tool_target": "adapter.noop.ca-llee!.tools.emit"},
		"bare target, no tool":   {"tool_target": "adapter.noop.callee.tools"},
		"tool_name conflict":     {"tool_target": "adapter.noop.callee.tools.emit", "tool_name": "other"},
		"args not an object":     {"tool_target": "adapter.noop.callee.tools.emit", "tool_args": "[1,2]"},
		"args not JSON":          {"tool_target": "adapter.noop.callee.tools.emit", "tool_args": "{bad"},
		"args JSON scalar":       {"tool_target": "adapter.noop.callee.tools.emit", "tool_args": `"x"`},
		"outputs with tool call": {"tool_target": "adapter.noop.callee.tools.emit", "outputs": "{}"},
	}
	for name, input := range cases {
		t.Run(name, func(t *testing.T) {
			caller := &noopService{sessions: map[string]struct{}{}}
			openSessions(t, caller, "caller")

			host := newToolTestHost(t)
			host.handle = func(requestID string, _ *structpb.Struct) []*v2.PermissionEvent {
				return []*v2.PermissionEvent{grantEvent(requestID)}
			}
			host.start(caller)
			defer host.stop()

			callCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			err := caller.Execute(callCtx, &v2.ExecuteRequest{
				SessionId: "caller",
				Input:     input,
			}, host.sink())
			if err == nil {
				t.Fatal("expected an error for malformed tool-call input")
			}
			if got := len(host.sentPayloads()); got != 0 {
				t.Errorf("malformed input sent %d payloads; want no wire traffic", got)
			}
			if got := len(host.resultEvents()); got != 0 {
				t.Errorf("malformed input emitted %d results; want none", got)
			}
		})
	}
}

// TestExecuteNoopOutputs covers the callee-side outputs input: a JSON object
// is emitted typed as the result's outputs_json, malformed values fail, and
// the default mode without outputs stays byte-compatible.
func TestExecuteNoopOutputs(t *testing.T) {
	ctx := context.Background()
	s := &noopService{sessions: map[string]struct{}{}}
	openSessions(t, s, "s1")

	t.Run("typed passthrough", func(t *testing.T) {
		sink := &captureSink{}
		input := map[string]string{
			"delay_ms": "0",
			"outputs":  `{"a":1,"s":"x","b":true,"n":{"k":[1,2]},"f":1.5}`,
		}
		if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: "s1", Input: input}, sink); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(sink.events) != 1 {
			t.Fatalf("got %d events, want 1", len(sink.events))
		}
		result := sink.events[0].GetResult()
		if got := result.GetOutcome(); got != "success" {
			t.Errorf("outcome = %q, want success", got)
		}
		want := map[string]any{
			"a": float64(1), "s": "x", "b": true,
			"n": map[string]any{"k": []any{float64(1), float64(2)}}, "f": 1.5,
		}
		if diff := jsonDiff(decodedOutputs(t, result), want); diff != "" {
			t.Errorf("outputs mismatch: %s", diff)
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: "s1", Input: map[string]string{"outputs": "{bad"}}, &captureSink{}); err == nil {
			t.Fatal("expected an error for malformed outputs JSON")
		}
	})

	t.Run("not an object", func(t *testing.T) {
		if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: "s1", Input: map[string]string{"outputs": "[1]"}}, &captureSink{}); err == nil {
			t.Fatal("expected an error for a non-object outputs value")
		}
	})

	t.Run("unset stays byte-compatible", func(t *testing.T) {
		sink := &captureSink{}
		if err := s.Execute(ctx, &v2.ExecuteRequest{SessionId: "s1", Input: map[string]string{}}, sink); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		if len(sink.events) != 1 {
			t.Fatalf("got %d events, want 1", len(sink.events))
		}
		result := sink.events[0].GetResult()
		if got := result.GetOutcome(); got != "success" {
			t.Errorf("outcome = %q, want success", got)
		}
		if len(result.GetOutputsJson()) != 0 {
			t.Errorf("outputs_json = %q, want empty (byte-compatible default mode)", result.GetOutputsJson())
		}
	})
}

// jsonDiff renders a JSON-semantic diff between two values.
func jsonDiff(got, want any) string {
	gotJSON, err := json.Marshal(got)
	if err != nil {
		return fmt.Sprintf("marshal got: %v", err)
	}
	wantJSON, err := json.Marshal(want)
	if err != nil {
		return fmt.Sprintf("marshal want: %v", err)
	}
	if string(gotJSON) != string(wantJSON) {
		return fmt.Sprintf("got %s, want %s", gotJSON, wantJSON)
	}
	return ""
}
