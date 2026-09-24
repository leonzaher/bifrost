package schemas

import (
	"crypto/rand"
	"sync"
)

// AgentApprovalContextKey holds a small handle to managed execution state.
const AgentApprovalContextKey BifrostContextKey = "agent-approval-state"

const AsyncJobStatusAwaitingApproval AsyncJobStatus = "awaiting_approval"

// Include an execution ID so concurrent requests with the same caller-supplied
// request ID cannot share approval state, and late cleanup cannot delete a new run.
type agentApprovalHandle struct {
	requestID   string
	executionID string
}

var agentApprovalStates sync.Map // map[agentApprovalHandle]*AgentApprovalState

// SetAgentApprovalState registers one execution with the manager. Only its handle
// enters the context; the returned cleanup releases this execution's state.
func SetAgentApprovalState(ctx *BifrostContext, state *AgentApprovalState) func() {
	requestID, _ := ctx.Value(BifrostContextKeyRequestID).(string)
	handle := agentApprovalHandle{requestID: requestID, executionID: rand.Text()}
	agentApprovalStates.Store(handle, state)
	ctx.SetValue(AgentApprovalContextKey, handle)
	return func() { agentApprovalStates.Delete(handle) }
}

// GetAgentApprovalState looks up the managed state for this execution.
func GetAgentApprovalState(ctx *BifrostContext) *AgentApprovalState {
	if ctx == nil {
		return nil
	}
	handle, ok := ctx.Value(AgentApprovalContextKey).(agentApprovalHandle)
	if !ok {
		return nil
	}
	state, _ := agentApprovalStates.Load(handle)
	approval, _ := state.(*AgentApprovalState)
	return approval
}

// ReleaseAgentApprovalState releases state after a run or a failed submission.
func ReleaseAgentApprovalState(ctx *BifrostContext) {
	if ctx != nil {
		if handle, ok := ctx.Value(AgentApprovalContextKey).(agentApprovalHandle); ok {
			agentApprovalStates.Delete(handle)
		}
	}
}

// AgentCheckpointContextKeys selects custom context values to persist across approvals.
// Set it to []BifrostContextKey. Only opt in small, non-sensitive JSON values;
// on resume, numbers become json.Number, arrays []any, and objects map[string]any.
// Custom Go types and runtime state are not restored. The selection survives resumes.
const AgentCheckpointContextKeys BifrostContextKey = "agent-checkpoint-context-keys"

const AsyncToolApprovalHeader = "x-bf-async-tool-approval"
const AsyncAutoToolsHeader = "x-bf-async-auto-tools"

// AgentCheckpoint preserves the request before the pending assistant response.
type AgentCheckpoint struct {
	ContextValues  map[BifrostContextKey]any      `json:"context_values,omitempty"`
	Request        *BifrostResponsesRequest       `json:"request"`
	Response       *BifrostResponsesResponse      `json:"response"`
	Pending        []ChatAssistantMessageToolCall `json:"pending"`
	Depth          int                            `json:"depth"`
	MaxDepth       int                            `json:"max_depth"`
	AutoTools      []string                       `json:"auto_tools"`
	IncludeTools   []string                       `json:"include_tools"`
	IncludeClients []string                       `json:"include_clients"`
}

// AgentApprovalState lives only for one execution; decisions apply to its first iteration.
type AgentApprovalState struct {
	Checkpoint *AgentCheckpoint
	Decisions  map[string]bool
	Depth      int
	MaxDepth   int
	AutoTools  []string

	FurtherInstructions map[string]string
}

// ToolApprovalRequest identifies an immutable saved group of tool calls.
type ToolApprovalRequest struct {
	ID    string                         `json:"tool_call_request_id"`
	Calls []ChatAssistantMessageToolCall `json:"calls"`
}

// ToolApprovalDecision decides one call in a saved approval request.
type ToolApprovalDecision struct {
	JobID      string `json:"job_id"`
	RequestID  string `json:"tool_call_request_id"`
	ToolCallID string `json:"tool_call_id"`
	Approved   *bool  `json:"approved"`

	FurtherInstructions string `json:"further_instructions,omitempty"`
}
