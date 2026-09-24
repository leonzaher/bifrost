package schemas

import "testing"

func TestAgentApprovalStateIsolationAndCleanup(t *testing.T) {
	first := NewBifrostContextWithValue(t.Context(), NoDeadline, BifrostContextKeyRequestID, "shared-request-id")
	second := NewBifrostContextWithValue(t.Context(), NoDeadline, BifrostContextKeyRequestID, "shared-request-id")
	firstState := &AgentApprovalState{Checkpoint: &AgentCheckpoint{Depth: 2}}
	secondState := &AgentApprovalState{Depth: 3}
	cleanupFirst := SetAgentApprovalState(first, firstState)
	t.Cleanup(cleanupFirst)
	t.Cleanup(SetAgentApprovalState(second, secondState))

	// Agent iterations change the request ID; the original execution remains addressable.
	first.SetValue(BifrostContextKeyRequestID, "next-turn")
	child := NewBifrostContext(first, NoDeadline)
	if GetAgentApprovalState(child) != firstState || GetAgentApprovalState(second) != secondState {
		t.Fatal("approval state was lost or shared between executions")
	}
	if child.Value(AgentApprovalContextKey) == firstState {
		t.Fatal("context retains approval contents")
	}
	ReleaseAgentApprovalState(child)
	if GetAgentApprovalState(first) != nil || GetAgentApprovalState(second) != secondState {
		t.Fatal("releasing an execution affected another request or retained its state")
	}
	// A late cleanup must not remove a later execution of the same request.
	t.Cleanup(SetAgentApprovalState(first, firstState))
	cleanupFirst()
	if GetAgentApprovalState(first) != firstState {
		t.Fatal("late cleanup removed a later execution")
	}
}
