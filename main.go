// Command criteria-adapter-noop is the standalone out-of-process noop adapter
// binary. It serves the protocol-v2 noop adapter via the public Go SDK: it
// opens sessions, optionally sleeps for delay_ms, and reports success. It runs
// no user code and exists for control-flow and pipeline scaffolding (and as the
// reference adapter for exercising the signing/lock path).
//
// A step whose input names a tool call (tool_target, tool_name, tool_args) is
// served by the tool-call caller mode: the adapter issues the call through the
// SDK's CallAdapterTool and passes the callee's outcome and outputs through
// its own ExecuteResult under callee.* keys. No LLM is involved; this mode is
// the reusable test double for adapter-tool conformance and examples.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	v2 "github.com/brokenbots/criteria-adapter-proto/criteria/v2"
	adapterhost "github.com/brokenbots/criteria-go-adapter-sdk/adapterhost"
)

// Version is the adapter's protocol-v2 version string.
const Version = "2.0.0"

// Input keys selecting the modes and their knobs.
const (
	inputDelayMS    = "delay_ms"
	inputToolTarget = "tool_target"
	inputToolName   = "tool_name"
	inputToolArgs   = "tool_args"
	inputOutputs    = "outputs"
)

// Output keys carrying the tool-call round-trip result (CRI-165).
const (
	calleeOutcomeKey = "callee.outcome"
	calleeOutputsKey = "callee.outputs"
	calleeErrorKey   = "callee.error"
)

type noopService struct {
	adapterhost.UnimplementedPermissions
	mu       sync.Mutex
	sessions map[string]struct{}

	toolBridgeOnce sync.Once
	toolBridge     *adapterhost.ToolCallBridge
}

// bridge returns the adapter's tool-call bridge, creating it on first use
// so adapters that never call tools keep the zero value.
func (s *noopService) bridge() *adapterhost.ToolCallBridge {
	s.toolBridgeOnce.Do(func() { s.toolBridge = adapterhost.NewToolCallBridge() })
	return s.toolBridge
}

// Permissions runs the tool-call dispatch loop (CRI-152): tool-call results,
// denials, and allow-grant ACKs all arrive on the Permissions stream, so
// CallAdapterTool calls can only complete while it runs.
func (s *noopService) Permissions(ctx context.Context, stream adapterhost.PermissionsStream) error {
	return s.bridge().Permissions(ctx, stream)
}

func (s *noopService) Info(context.Context, *v2.InfoRequest) (*v2.InfoResponse, error) {
	return &v2.InfoResponse{
		Name:               "noop",
		Version:            Version,
		SourceUrl:          "https://github.com/brokenbots/criteria-adapter-noop",
		SdkProtocolVersion: "2",
		Platforms:          []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"},
		Capabilities:       []string{"parallel_safe", adapterhost.CapabilityAdapterTools},
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
	input := request.GetInput()
	s.mu.Lock()
	_, ok := s.sessions[request.GetSessionId()]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("unknown session %q", request.GetSessionId())
	}

	if rawDelay := input[inputDelayMS]; rawDelay != "" {
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

	if input[inputToolTarget] != "" {
		if input[inputOutputs] != "" {
			return fmt.Errorf("inputs %q and %q are mutually exclusive", inputToolTarget, inputOutputs)
		}
		return s.executeToolCall(ctx, request.GetSessionId(), input, sink)
	}
	return executeNoop(input, sink)
}

// executeNoop serves the default mode: a single success result, with typed
// outputs when the step's outputs input carries a JSON object (the callee-side
// double a caller noop reads back through callee.outputs).
func executeNoop(input map[string]string, sink adapterhost.ExecuteEventSender) error {
	result := &v2.ExecuteResult{Outcome: "success"}
	if raw := input[inputOutputs]; raw != "" {
		outputs, err := decodeJSONObject(raw, inputOutputs)
		if err != nil {
			return err
		}
		encoded, err := json.Marshal(outputs)
		if err != nil {
			return fmt.Errorf("encode %s: %w", inputOutputs, err)
		}
		result.OutputsJson = encoded
	}
	return sendResult(sink, result)
}

// executeToolCall serves the tool-call caller mode: the named target+tool+args
// are issued as a CallAdapterTool and the callee's result passes through under
// callee.* keys. A failed round-trip (denied, unknown adapter, callee crash,
// ...) is reported as the failure outcome with callee.error, not an Execute
// error: the step ran and produced a result.
func (s *noopService) executeToolCall(ctx context.Context, sessionID string, input map[string]string, sink adapterhost.ExecuteEventSender) error {
	target, args, err := toolCallFromInput(input)
	if err != nil {
		return err
	}
	calleeOutcome, calleeOutputs, err := s.bridge().CallAdapterTool(ctx, sink, adapterhost.AdapterToolCall{
		SessionID: sessionID,
		Target:    adapterhost.FormatAdapterToolTarget(target.adapterType, target.instance, target.name),
		Tool:      target.name,
		Args:      args,
	})
	if err != nil {
		outputs, encErr := encodeOutputs(map[string]any{calleeErrorKey: err.Error()})
		if encErr != nil {
			return encErr
		}
		return sendResult(sink, &v2.ExecuteResult{
			Outcome:     "failure",
			OutputsJson: outputs,
		})
	}
	if calleeOutputs == nil {
		calleeOutputs = map[string]any{}
	}
	outputs, err := encodeOutputs(map[string]any{
		calleeOutcomeKey: calleeOutcome,
		calleeOutputsKey: calleeOutputs,
	})
	if err != nil {
		return err
	}
	return sendResult(sink, &v2.ExecuteResult{Outcome: "success", OutputsJson: outputs})
}

// toolCallFromInput validates the tool-call inputs and renders the §8 call
// arguments. The target's tool segment wins; tool_name names the tool for a
// bare target (adapter.<type>.<name>.tools) and must agree with the segment
// when both are present.
func toolCallFromInput(input map[string]string) (toolCallTarget, map[string]any, error) {
	target, err := parseToolTarget(input[inputToolTarget])
	if err != nil {
		return toolCallTarget{}, nil, err
	}
	if name := input[inputToolName]; name != "" {
		switch {
		case target.name == "":
			target.name = name
		case target.name != name:
			return toolCallTarget{}, nil, fmt.Errorf("tool_name %q conflicts with tool target %q", name, input[inputToolTarget])
		}
	}
	if target.name == "" {
		return toolCallTarget{}, nil, fmt.Errorf("bare tool target %q requires tool_name", input[inputToolTarget])
	}
	args := map[string]any{}
	if raw := input[inputToolArgs]; raw != "" {
		decoded, err := decodeJSONObject(raw, inputToolArgs)
		if err != nil {
			return toolCallTarget{}, nil, err
		}
		args = decoded
	}
	return target, args, nil
}

// toolCallTarget is the parsed §2 tool target adapter.<type>.<name>.tools[.<tool>].
type toolCallTarget struct {
	adapterType string
	instance    string
	name        string
}

// parseToolTarget parses the strict §2 tool-target form, mirroring the
// engine's grammar so malformed targets fail before any wire traffic.
func parseToolTarget(raw string) (toolCallTarget, error) {
	labels := strings.Split(raw, ".")
	if len(labels) != 4 && len(labels) != 5 {
		return toolCallTarget{}, fmt.Errorf("invalid tool_target %q: want adapter.<type>.<name>.tools[.<tool>]", raw)
	}
	for _, label := range labels {
		if !isBarewordLabel(label) {
			return toolCallTarget{}, fmt.Errorf("invalid tool_target %q: label %q is not a bareword", raw, label)
		}
	}
	if labels[0] != "adapter" || labels[3] != "tools" {
		return toolCallTarget{}, fmt.Errorf("invalid tool_target %q: want adapter.<type>.<name>.tools[.<tool>]", raw)
	}
	target := toolCallTarget{adapterType: labels[1], instance: labels[2]}
	if len(labels) == 5 {
		target.name = labels[4]
	}
	return target, nil
}

// isBarewordLabel mirrors the engine's bareword identifier: a leading letter
// or underscore followed by letters, digits, underscores, or hyphens.
func isBarewordLabel(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
			// always allowed
		case c >= '0' && c <= '9', c == '-':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// decodeJSONObject decodes a JSON-object input value, rejecting every other
// JSON shape.
func decodeJSONObject(raw, name string) (map[string]any, error) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	if obj == nil {
		return nil, fmt.Errorf("invalid %s: want a JSON object", name)
	}
	return obj, nil
}

// encodeOutputs renders an ExecuteResult.outputs_json payload.
func encodeOutputs(outputs map[string]any) ([]byte, error) {
	return json.Marshal(outputs)
}

func sendResult(sink adapterhost.ExecuteEventSender, result *v2.ExecuteResult) error {
	return sink.Send(&v2.ExecuteEvent{Event: &v2.ExecuteEvent_Result{Result: result}})
}

func (s *noopService) Log(context.Context, *v2.LogRequest, adapterhost.LogEventSender) error {
	// The SDK owns the log stream and its heartbeat for the full session
	// lifetime, so Log returns immediately instead of parking on the context
	// (pre-CRI-164 SDKs needed the adapter to hold the stream open).
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
