package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/maximhq/bifrost/core/schemas"
)

type approvalClientManager struct{ MockClientManager }

func (*approvalClientManager) GetClientForTool(name string) *schemas.MCPClientState {
	return &schemas.MCPClientState{ExecutionConfig: &schemas.MCPClientConfig{
		Name: "test", ToolsToExecute: []string{"*"}, ToolsToAutoExecute: []string{"automatic"},
	}}
}

func TestApprovalRequiresObjectArguments(t *testing.T) {
	for _, tc := range []struct {
		arguments string
		valid     bool
	}{
		{`{}`, true},
		{" \n\t{\"nested\": [1, null]}\r ", true},
		{`null`, false},
		{`[]`, false},
		{`"{}"`, false},
		{`{"broken":}`, false},
		{`{} {}`, false},
		{"", false},
	} {
		t.Run(tc.arguments, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			state := &schemas.AgentApprovalState{AutoTools: []string{}}
			t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
			response := &schemas.BifrostResponsesResponse{Output: []schemas.ResponsesMessage{{
				Type: schemas.Ptr(schemas.ResponsesMessageTypeFunctionCall),
				ResponsesToolMessage: &schemas.ResponsesToolMessage{
					CallID: schemas.Ptr("call"), Name: schemas.Ptr("test-manual"), Arguments: &tc.arguments,
				},
			}}}
			executor := &AgentModeExecutor{logger: &MockLogger{}}
			_, err := executor.ExecuteAgentForResponsesRequest(ctx, 1, &schemas.BifrostResponsesRequest{}, response,
				nil, nil, nil, &approvalClientManager{})
			if (err == nil) != tc.valid || (state.Checkpoint != nil) != tc.valid {
				t.Fatalf("valid=%v: checkpoint=%v error=%v", tc.valid, state.Checkpoint != nil, err)
			}
		})
	}
}

func TestApprovalAutoToolsAppliesToCodeMode(t *testing.T) {
	for _, toolName := range []string{ToolTypeExecuteToolCode, ToolTypeListToolFiles} {
		t.Run(toolName, func(t *testing.T) {
			ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			state := &schemas.AgentApprovalState{AutoTools: []string{}}
			t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
			response := &schemas.BifrostResponsesResponse{Output: []schemas.ResponsesMessage{{
				Type: schemas.Ptr(schemas.ResponsesMessageTypeFunctionCall),
				ResponsesToolMessage: &schemas.ResponsesToolMessage{
					CallID: schemas.Ptr("call"), Name: &toolName, Arguments: schemas.Ptr(`{"code":"print(42)"}`),
				},
			}}}
			executions := 0
			nestedAllowed := make(chan bool, 2)
			config := &schemas.MCPClientConfig{Name: "test", ToolsToExecute: []string{"manual"}}
			runTool := func(toolCtx *schemas.BifrostContext, _ *schemas.BifrostMCPRequest) (*schemas.BifrostMCPResponse, error) {
				executions++
				nestedAllowed <- AuthorizeCodeModeToolCall(toolCtx, "test-manual", config) == nil
				return nil, nil
			}
			next := func(*schemas.BifrostContext, *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
				return &schemas.BifrostResponsesResponse{}, nil
			}
			executor := &AgentModeExecutor{logger: &MockLogger{}}
			_, err := executor.ExecuteAgentForResponsesRequest(ctx, 1, &schemas.BifrostResponsesRequest{}, response,
				next, nil, runTool, &MockClientManager{})
			if err != nil || executions != 0 || state.Checkpoint == nil {
				t.Fatalf("code-mode tool bypassed approval: executions=%d checkpoint=%v error=%v", executions, state.Checkpoint != nil, err)
			}
			state.AutoTools = []string{toolName}
			_, err = executor.ExecuteAgentForResponsesRequest(ctx, 1, &schemas.BifrostResponsesRequest{}, response,
				next, nil, runTool, &MockClientManager{})
			if err != nil || executions != 1 || state.Checkpoint != nil {
				t.Fatalf("explicit auto tool was blocked: executions=%d checkpoint=%v error=%v", executions, state.Checkpoint != nil, err)
			}
			if <-nestedAllowed {
				t.Fatal("automatic code-mode execution bypassed nested tool approval")
			}
			state.AutoTools = []string{}
			state.Decisions = map[string]bool{"call": true}
			_, err = executor.ExecuteAgentForResponsesRequest(ctx, 1, &schemas.BifrostResponsesRequest{}, response,
				next, nil, runTool, &MockClientManager{})
			if err != nil || executions != 2 || state.Checkpoint != nil {
				t.Fatalf("approved code-mode tool was blocked: executions=%d checkpoint=%v error=%v", executions, state.Checkpoint != nil, err)
			}
			if !<-nestedAllowed {
				t.Fatal("human-approved code-mode execution was still treated as unattended")
			}
		})
	}
}

func TestApprovalCheckpointIncludesPriorTurnsAndMixedCalls(t *testing.T) {
	toolResponse := func(ids ...string) *schemas.BifrostResponsesResponse {
		response := &schemas.BifrostResponsesResponse{}
		for _, id := range ids {
			name := "test-automatic"
			if id == "manual" || id == "manual-other" {
				name = "test-manual"
			}
			response.Output = append(response.Output, schemas.ResponsesMessage{
				Type:                 schemas.Ptr(schemas.ResponsesMessageTypeFunctionCall),
				ResponsesToolMessage: &schemas.ResponsesToolMessage{CallID: schemas.Ptr(id), Name: &name, Arguments: schemas.Ptr("{}")},
			})
		}
		return response
	}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	state := &schemas.AgentApprovalState{}
	t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
	executor := &AgentModeExecutor{logger: &MockLogger{}}
	request := &schemas.BifrostResponsesRequest{Input: []schemas.ResponsesMessage{{
		Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser), Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("original prompt")},
	}}}
	executed := make(chan string, 4)
	runTool := func(_ *schemas.BifrostContext, req *schemas.BifrostMCPRequest) (*schemas.BifrostMCPResponse, error) {
		call := *req.ChatAssistantMessageToolCall
		executed <- *call.ID
		return &schemas.BifrostMCPResponse{ChatMessage: createToolResultMessage(call, "done", nil)}, nil
	}
	_, err := executor.ExecuteAgentForResponsesRequest(ctx, 3, request, toolResponse("first"),
		func(*schemas.BifrostContext, *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
			return toolResponse("second", "manual", "manual-other"), nil
		}, nil, runTool, &approvalClientManager{})
	if err != nil {
		t.Fatal(err)
	}
	if len(executed) != 1 || state.Checkpoint == nil {
		t.Fatal("mixed group executed before approval")
	}
	if ctx.Value(schemas.AgentApprovalContextKey) == state {
		t.Fatal("context retains the full approval checkpoint instead of a manager handle")
	}
	if len(state.Checkpoint.Request.Input) != 3 || state.Checkpoint.Depth != 1 {
		t.Fatal("prior turn or depth missing")
	}
	data, marshalErr := json.Marshal(state.Checkpoint)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	var cp schemas.AgentCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		t.Fatal(err)
	}
	state = &schemas.AgentApprovalState{
		Depth: cp.Depth, MaxDepth: cp.MaxDepth, Decisions: map[string]bool{"manual": false, "manual-other": false},
		FurtherInstructions: map[string]string{"manual-other": "Leave the other item alone.", "manual": "Try another approach."},
	}
	t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
	continuations := 0
	_, err = executor.ExecuteAgentForResponsesRequest(ctx, 3, cp.Request, cp.Response,
		func(_ *schemas.BifrostContext, req *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
			if len(req.Input) != 11+2*continuations {
				t.Errorf("continuation lost history or repeated feedback: %d", len(req.Input))
				return &schemas.BifrostResponsesResponse{}, nil
			}
			for _, msg := range req.Input[6:9] {
				if msg.Type == nil || *msg.Type != schemas.ResponsesMessageTypeFunctionCallOutput {
					t.Error("feedback must follow all tool results")
				}
			}
			for i, text := range []string{
				"Regarding rejected tool call manual (test-manual):\nTry another approach.",
				"Regarding rejected tool call manual-other (test-manual):\nLeave the other item alone.",
			} {
				msg := req.Input[9+i]
				if msg.Role == nil || *msg.Role != schemas.ResponsesInputMessageRoleUser ||
					msg.Content == nil || msg.Content.ContentStr == nil || *msg.Content.ContentStr != text {
					t.Errorf("missing or reordered user feedback at input %d", 9+i)
				}
			}
			continuations++
			if continuations == 1 {
				return toolResponse("third"), nil
			}
			return &schemas.BifrostResponsesResponse{}, nil
		}, nil, runTool, &approvalClientManager{})
	if err != nil {
		t.Fatal(err)
	}
	if len(executed) != 3 || continuations != 2 {
		t.Fatal("repeated prior automatic tool or executed rejected tool")
	}
	if <-executed != "first" || <-executed != "second" || <-executed != "third" {
		t.Fatal("unexpected executions")
	}
}

func TestDurableApprovalUsesSavedCallOnly(t *testing.T) {
	manager := NewMCPManager(context.Background(), schemas.MCPConfig{}, nil, nil, nil)
	t.Cleanup(func() { _ = manager.Cleanup() })
	var executions int
	err := manager.RegisterTool("add_numbers", "Add numbers", func(any) (string, error) {
		executions++
		return "42", nil
	}, schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "add_numbers"}})
	if err != nil {
		t.Fatal(err)
	}
	request := &schemas.BifrostResponsesRequest{Input: []schemas.ResponsesMessage{{
		Role:    schemas.Ptr(schemas.ResponsesInputMessageRoleUser),
		Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("Calculate 19 + 23.")},
	}}}
	response := &schemas.BifrostResponsesResponse{Output: []schemas.ResponsesMessage{{
		Type: schemas.Ptr(schemas.ResponsesMessageTypeFunctionCall),
		ResponsesToolMessage: &schemas.ResponsesToolMessage{
			CallID: schemas.Ptr("call_add_1"), Name: schemas.Ptr("bifrostInternal-add_numbers"),
			Arguments: schemas.Ptr(`{"a":19,"b":23}`),
		},
	}}}
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	state := &schemas.AgentApprovalState{AutoTools: []string{}}
	t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
	next := func(_ *schemas.BifrostContext, req *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
		if len(req.Input) != 3 {
			t.Errorf("restored history has %d items", len(req.Input))
		}
		return response, nil // Reusing an ID must not reuse approval.
	}
	_, bfErr := manager.CheckAndExecuteAgentForResponsesRequest(ctx, request, response, next)
	if bfErr != nil || state.Checkpoint == nil || executions != 0 {
		t.Fatalf("did not pause: %v", bfErr)
	}
	data, err := json.Marshal(state.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	var restored schemas.AgentCheckpoint
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatal(err)
	}
	state = &schemas.AgentApprovalState{
		Decisions: map[string]bool{"call_add_1": true}, MaxDepth: restored.MaxDepth,
		Depth: restored.Depth, AutoTools: restored.AutoTools,
	}
	t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
	_, bfErr = manager.CheckAndExecuteAgentForResponsesRequest(ctx, restored.Request, restored.Response, next)
	if bfErr != nil || executions != 1 || state.Checkpoint == nil {
		t.Fatalf("approval leaked or did not execute: count=%d error=%v", executions, bfErr)
	}
}
