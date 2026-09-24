package logstore

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/mcp"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/grant"
	"github.com/stretchr/testify/require"
)

func TestAgentApprovalRestoresAndExecutesOnce(t *testing.T) {
	for _, tt := range []struct {
		name         string
		allow        bool
		tools        any
		executions   int32
		instructions string
	}{
		{name: "approved", allow: true, tools: []string{"bifrostInternal-add"}, executions: 1},
		{name: "rejected", tools: []string{"bifrostInternal-add"}},
		{
			name: "rejected with feedback", tools: []string{"bifrostInternal-add"},
			instructions: "Explain the calculation without calling a tool.",
		},
		{name: "empty filter denies approved tool", allow: true, tools: []string{}},
		{name: "nil slice denies approved tool", allow: true, tools: []string(nil)},
		{name: "absent filter preserves defaults", allow: true, executions: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			executor := newTestAsyncExecutor(t)
			manager := mcp.NewMCPManager(context.Background(), schemas.MCPConfig{}, nil, nil, nil)
			t.Cleanup(func() { _ = manager.Cleanup() })
			var executed atomic.Int32
			require.NoError(t, manager.RegisterTool("add", "Add", func(any) (string, error) {
				executed.Add(1)
				return "42", nil
			}, schemas.ChatTool{Type: schemas.ChatToolTypeFunction, Function: &schemas.ChatToolFunction{Name: "add"}}))
			ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			ctx.SetValue(schemas.BifrostContextKeyVirtualKey, "sk-bf-test")
			g := grant.New()
			require.True(t, ctx.SetGrant(g))
			if tt.tools != nil {
				ctx.SetValue(schemas.MCPContextKeyIncludeTools, tt.tools)
			}
			t.Cleanup(schemas.SetAgentApprovalState(ctx, &schemas.AgentApprovalState{AutoTools: []string{}}))
			request := &schemas.BifrostResponsesRequest{Provider: schemas.OpenAI, Model: "test", Input: []schemas.ResponsesMessage{{
				Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser), Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr("19 + 23")},
			}}}
			response := &schemas.BifrostResponsesResponse{Output: []schemas.ResponsesMessage{{
				Type: schemas.Ptr(schemas.ResponsesMessageTypeFunctionCall), ResponsesToolMessage: &schemas.ResponsesToolMessage{
					CallID: schemas.Ptr("call-1"), Name: schemas.Ptr("bifrostInternal-add"), Arguments: schemas.Ptr("{}"),
				},
			}}}
			continued := make(chan *schemas.BifrostResponsesRequest, 1)
			next := func(_ *schemas.BifrostContext, req *schemas.BifrostResponsesRequest) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
				continued <- req
				return &schemas.BifrostResponsesResponse{}, nil
			}
			job, err := executor.SubmitJob(ctx, 60, func(bg *schemas.BifrostContext) (any, *schemas.BifrostError) {
				return manager.CheckAndExecuteAgentForResponsesRequest(bg, request, response, next)
			}, schemas.ResponsesRequest)
			require.NoError(t, err)
			var pending *AsyncJob
			require.Eventually(t, func() bool {
				pending, err = executor.RetrieveJob(ctx, job.ID, schemas.Ptr("sk-bf-test"), schemas.ResponsesRequest)
				return err == nil && pending.Status == schemas.AsyncJobStatusAwaitingApproval
			}, 5*time.Second, 10*time.Millisecond)
			require.Zero(t, executed.Load())
			require.NotNil(t, pending.Approval)
			// A fresh executor has no original request closure or live context.
			restored := NewAsyncJobExecutor(executor.logstore, executor.governanceStore, nil, nil, asyncTestLogger{})
			resume := func(bg *schemas.BifrostContext, cp *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
				if bg.Grant() != g {
					t.Error("resumed job lost the approval request's grant")
				}
				return manager.CheckAndExecuteAgentForResponsesRequest(bg, cp.Request, cp.Response, next)
			}
			decision := schemas.ToolApprovalDecision{
				JobID: job.ID, RequestID: pending.Approval.ID, ToolCallID: "wrong", Approved: schemas.Ptr(tt.allow),
			}
			_, err = restored.ApproveAgent(ctx, decision, resume)
			require.ErrorIs(t, err, ErrInvalidApproval)
			decision.ToolCallID = "call-1"
			decision.FurtherInstructions = tt.instructions
			unauthorized := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
			_, err = restored.ApproveAgent(unauthorized, decision, resume)
			require.Error(t, err)
			changedScope := schemas.NewBifrostContext(ctx, schemas.NoDeadline)
			changedScope.SetValue(schemas.BifrostContextKeyMCPExtraHeaders, map[string][]string{"x-organization-id": {"different"}})
			_, err = restored.ApproveAgent(changedScope, decision, resume)
			require.ErrorContains(t, err, "scope does not match")
			var accepted atomic.Int32
			var unexpected atomic.Int32
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					if _, err := restored.ApproveAgent(ctx, decision, resume); err == nil {
						accepted.Add(1)
					} else {
						unexpected.Add(1)
					}
				})
			}
			wg.Wait()
			require.EqualValues(t, 4, accepted.Load())
			require.Zero(t, unexpected.Load(), "identical approvals must succeed")
			require.Equal(t, schemas.AsyncJobStatusCompleted, waitForJobStatus(t, executor.logstore, job.ID).Status)
			require.Equal(t, tt.executions, executed.Load())
			select {
			case req := <-continued:
				if tt.instructions == "" {
					require.Len(t, req.Input, 3)
				} else {
					require.Len(t, req.Input, 4)
					require.Equal(t, schemas.ResponsesInputMessageRoleUser, *req.Input[3].Role)
					require.Contains(t, *req.Input[3].Content.ContentStr, "call-1")
					require.Contains(t, *req.Input[3].Content.ContentStr, tt.instructions)
				}
				require.Equal(t, "call-1", *req.Input[2].CallID)
			case <-time.After(time.Second):
				t.Fatal("missing continuation")
			}
			db, err := approvalDB(executor.logstore)
			require.NoError(t, err)
			var record AgentApproval
			require.NoError(t, db.First(&record, "id = ?", decision.RequestID).Error)
			require.NotNil(t, record.DecidedAt)
			require.NotEmpty(t, record.Decisions)
		})
	}
}

func TestAgentApprovalCollectsIndividualDecisions(t *testing.T) {
	executor := newTestAsyncExecutor(t)
	ctx := schemas.NewBifrostContext(t.Context(), schemas.NoDeadline)
	job := &AsyncJob{
		ID: "individual-decisions", Status: schemas.AsyncJobStatusProcessing,
		RequestType: schemas.ResponsesRequest, ResultTTL: 60, CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, executor.logstore.CreateAsyncJob(ctx, job))
	scope, err := approvalScope(ctx)
	require.NoError(t, err)
	require.NoError(t, executor.pauseAgent(ctx, job, &schemas.AgentCheckpoint{
		Request: &schemas.BifrostResponsesRequest{}, Response: &schemas.BifrostResponsesResponse{},
		Pending: []schemas.ChatAssistantMessageToolCall{
			{ID: schemas.Ptr("first")}, {ID: schemas.Ptr("second")}, {ID: schemas.Ptr("third")},
		},
	}, scope))
	pending, err := executor.RetrieveJob(ctx, job.ID, nil, schemas.ResponsesRequest)
	require.NoError(t, err)
	var executions atomic.Int32
	resumed := make(chan *schemas.AgentApprovalState, 1)
	resume := func(bg *schemas.BifrostContext, _ *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
		executions.Add(1)
		resumed <- schemas.GetAgentApprovalState(bg)
		return &schemas.BifrostResponsesResponse{}, nil
	}
	input := schemas.ToolApprovalDecision{
		JobID: job.ID, RequestID: pending.Approval.ID, ToolCallID: "first",
		FurtherInstructions: "  Try another approach.  ",
	}
	_, err = executor.ApproveAgent(ctx, input, resume)
	require.ErrorIs(t, err, ErrInvalidApproval)
	input.Approved = schemas.Ptr(true)
	_, err = executor.ApproveAgent(ctx, input, resume)
	require.ErrorIs(t, err, ErrInvalidApproval, "instructions require a rejected decision")
	input.Approved = schemas.Ptr(false)
	result, err := executor.ApproveAgent(ctx, input, resume)
	require.NoError(t, err)
	require.Equal(t, schemas.AsyncJobStatusAwaitingApproval, result.Status)
	require.Zero(t, executions.Load())
	db, err := approvalDB(executor.logstore)
	require.NoError(t, err)
	var saved AgentApproval
	require.NoError(t, db.First(&saved, "id = ?", input.RequestID).Error)
	require.Nil(t, saved.DecidedAt)
	require.Equal(t, map[string]bool{"first": false}, saved.Decisions)
	require.Equal(t, map[string]string{"first": "Try another approach."}, saved.FurtherInstructions)
	result, err = executor.ApproveAgent(ctx, input, resume)
	require.NoError(t, err)
	require.Len(t, result.Approval.Calls, 3)
	input.FurtherInstructions = "Different instructions."
	_, err = executor.ApproveAgent(ctx, input, resume)
	require.ErrorIs(t, err, ErrApprovalConflict, "retrying cannot change the feedback")
	input.FurtherInstructions = ""
	input.Approved = schemas.Ptr(true)
	_, err = executor.ApproveAgent(ctx, input, resume)
	require.ErrorIs(t, err, ErrApprovalConflict)

	// Separate executors must merge concurrent submissions without losing decisions.
	restored := NewAsyncJobExecutor(executor.logstore, executor.governanceStore, nil, nil, asyncTestLogger{})
	errs := make(chan error, 4)
	var wg sync.WaitGroup
	for _, approver := range []*AsyncJobExecutor{executor, restored} {
		for _, callID := range []string{"second", "third"} {
			wg.Go(func() {
				_, err := approver.ApproveAgent(ctx, schemas.ToolApprovalDecision{
					JobID: job.ID, RequestID: input.RequestID, ToolCallID: callID, Approved: schemas.Ptr(false),
					FurtherInstructions: "Feedback for " + callID,
				}, resume)
				errs <- err
			})
		}
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.Equal(t, schemas.AsyncJobStatusCompleted, waitForJobStatus(t, executor.logstore, job.ID).Status)
	require.EqualValues(t, 1, executions.Load())
	state := <-resumed
	require.Equal(t, map[string]bool{"first": false, "second": false, "third": false}, state.Decisions)
	require.Equal(t, map[string]string{
		"first": "Try another approach.", "second": "Feedback for second", "third": "Feedback for third",
	}, state.FurtherInstructions)
	input.Approved = schemas.Ptr(false)
	input.FurtherInstructions = "Try another approach."
	result, err = executor.ApproveAgent(ctx, input, resume)
	require.NoError(t, err)
	require.Equal(t, schemas.AsyncJobStatusCompleted, result.Status)
}

func TestApprovalSurvivesDatabaseReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	ctx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	ctx.SetValue(schemas.MCPContextKeyIncludeTools, []string{"saved-tool"})
	ctx.SetValue(schemas.MCPContextKeyIncludeClients, []string{"saved-client"})
	inputLoggedKey := schemas.BifrostContextKey("app-input-logged")
	labelKey := schemas.BifrostContextKey("app-label")
	checkpointKeys := schemas.AgentCheckpointContextKeys
	ctx.SetValue(labelKey, "original")
	ctx.SetValue(schemas.BifrostContextKey("unselected-value"), "must-not-persist")
	store, err := newSqliteLogStore(ctx, &SQLiteConfig{Path: path}, asyncTestLogger{})
	require.NoError(t, err)
	executor := NewAsyncJobExecutor(store, nil, nil, nil, asyncTestLogger{})
	state := &schemas.AgentApprovalState{}
	t.Cleanup(schemas.SetAgentApprovalState(ctx, state))
	job, err := executor.SubmitJob(ctx, 60, func(bg *schemas.BifrostContext) (any, *schemas.BifrostError) {
		name := "test-plugin"
		scoped := bg.WithPluginScope(&name)
		schemas.AppendToContextList(scoped, checkpointKeys, inputLoggedKey)
		schemas.AppendToContextList(scoped, checkpointKeys, labelKey)
		scoped.SetValue(inputLoggedKey, true)
		scoped.ReleasePluginScope()
		state.Checkpoint = &schemas.AgentCheckpoint{
			Request:  &schemas.BifrostResponsesRequest{Model: "saved-model"},
			Response: &schemas.BifrostResponsesResponse{},
			Pending:  []schemas.ChatAssistantMessageToolCall{{ID: schemas.Ptr("first-call")}, {ID: schemas.Ptr("saved-call")}},
			Depth:    2, MaxDepth: 5,
		}
		return state.Checkpoint.Response, nil
	}, schemas.ResponsesRequest)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		j, err := executor.RetrieveJob(ctx, job.ID, nil, schemas.ResponsesRequest)
		return err == nil && j.Status == schemas.AsyncJobStatusAwaitingApproval
	}, 5*time.Second, 10*time.Millisecond)
	var saved AgentApproval
	require.NoError(t, store.db.First(&saved, "job_id = ?", job.ID).Error)
	require.NotContains(t, saved.Checkpoint, "must-not-persist")
	var checkpoint map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(saved.Checkpoint), &checkpoint))
	require.JSONEq(t, `{"app-input-logged":true,"app-label":"original"}`, string(checkpoint["context_values"]))
	partial, err := executor.ApproveAgent(ctx, schemas.ToolApprovalDecision{
		JobID: job.ID, RequestID: saved.ID, ToolCallID: "first-call", Approved: schemas.Ptr(false),
		FurtherInstructions: "Use a different approach after restarting.",
	}, func(*schemas.BifrostContext, *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
		t.Error("resumed before every call was decided")
		return &schemas.BifrostResponsesResponse{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, schemas.AsyncJobStatusAwaitingApproval, partial.Status)
	require.NoError(t, store.Close(ctx))
	store, err = newSqliteLogStore(ctx, &SQLiteConfig{Path: path}, asyncTestLogger{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(context.Background()) })
	executor = NewAsyncJobExecutor(store, nil, nil, nil, asyncTestLogger{})
	freshCtx := schemas.NewBifrostContext(context.Background(), schemas.NoDeadline)
	freshCtx.SetValue(inputLoggedKey, false)
	freshCtx.SetValue(labelKey, "replacement")
	freshCtx.SetValue(schemas.MCPContextKeyIncludeTools, []string{"*"})
	t.Cleanup(schemas.SetAgentApprovalState(freshCtx, &schemas.AgentApprovalState{AutoTools: []string{"*"}}))
	pending, err := executor.RetrieveJob(freshCtx, job.ID, nil, schemas.ResponsesRequest)
	require.NoError(t, err)
	restored := make(chan *schemas.AgentCheckpoint, 1)
	_, err = executor.ApproveAgent(freshCtx, schemas.ToolApprovalDecision{
		JobID: job.ID, RequestID: pending.Approval.ID, ToolCallID: "saved-call", Approved: schemas.Ptr(false),
	}, func(bg *schemas.BifrostContext, cp *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
		require.Equal(t, true, bg.Value(inputLoggedKey))
		require.Equal(t, "original", bg.Value(labelKey))
		require.ElementsMatch(t, []schemas.BifrostContextKey{inputLoggedKey, labelKey}, bg.Value(checkpointKeys))
		require.Nil(t, bg.Value(schemas.BifrostContextKey("unselected-value")))
		require.Equal(t, []string{"saved-tool"}, bg.Value(schemas.MCPContextKeyIncludeTools))
		require.Equal(t, []string{"saved-client"}, bg.Value(schemas.MCPContextKeyIncludeClients))
		require.Empty(t, schemas.GetAgentApprovalState(bg).AutoTools)
		require.Equal(t, map[string]bool{"first-call": false, "saved-call": false},
			schemas.GetAgentApprovalState(bg).Decisions)
		require.Equal(t, map[string]string{"first-call": "Use a different approach after restarting."},
			schemas.GetAgentApprovalState(bg).FurtherInstructions)
		restored <- cp
		return &schemas.BifrostResponsesResponse{}, nil
	})
	require.NoError(t, err)
	require.Equal(t, schemas.AsyncJobStatusCompleted, waitForJobStatus(t, store, job.ID).Status)
	cp := <-restored
	require.Equal(t, "saved-model", cp.Request.Model)
	require.Equal(t, 2, cp.Depth)
	require.Equal(t, 5, cp.MaxDepth)
}

func TestApprovalCheckpointContextAcrossPauses(t *testing.T) {
	executor := newTestAsyncExecutor(t)
	ctx := schemas.NewBifrostContext(t.Context(), schemas.NoDeadline)
	flag := schemas.BifrostContextKey("app-input-logged")
	number := schemas.BifrostContextKey("app-number")
	ctx.SetValue(schemas.AgentCheckpointContextKeys, []schemas.BifrostContextKey{flag, number})
	ctx.SetValue(flag, true)
	ctx.SetValue(number, int64(9007199254740993))
	t.Cleanup(schemas.SetAgentApprovalState(ctx, &schemas.AgentApprovalState{}))
	pause := func(bg *schemas.BifrostContext) (any, *schemas.BifrostError) {
		state := schemas.GetAgentApprovalState(bg)
		state.Checkpoint = &schemas.AgentCheckpoint{
			Request: &schemas.BifrostResponsesRequest{}, Response: &schemas.BifrostResponsesResponse{},
			Pending: []schemas.ChatAssistantMessageToolCall{{ID: schemas.Ptr("call")}},
		}
		return state.Checkpoint.Response, nil
	}
	job, err := executor.SubmitJob(ctx, 60, pause, schemas.ResponsesRequest)
	require.NoError(t, err)
	for round := range 2 {
		var pending *AsyncJob
		require.Eventually(t, func() bool {
			pending, err = executor.RetrieveJob(ctx, job.ID, nil, schemas.ResponsesRequest)
			return err == nil && pending.Status == schemas.AsyncJobStatusAwaitingApproval
		}, 5*time.Second, 10*time.Millisecond)
		fresh := schemas.NewBifrostContext(t.Context(), schemas.NoDeadline)
		captured := make(chan [2]any, 1)
		_, err := executor.ApproveAgent(fresh, schemas.ToolApprovalDecision{
			JobID: job.ID, RequestID: pending.Approval.ID, ToolCallID: "call", Approved: schemas.Ptr(true),
		}, func(bg *schemas.BifrostContext, cp *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
			captured <- [2]any{bg.Value(flag), bg.Value(number)}
			if round == 0 {
				bg.SetValue(flag, false)
				pause(bg)
			}
			return &schemas.BifrostResponsesResponse{}, nil
		})
		require.NoError(t, err)
		select {
		case values := <-captured:
			require.Equal(t, round == 0, values[0])
			require.Equal(t, json.Number("9007199254740993"), values[1])
		case <-time.After(5 * time.Second):
			t.Fatal("resume did not run")
		}
	}
	require.Equal(t, schemas.AsyncJobStatusCompleted, waitForJobStatus(t, executor.logstore, job.ID).Status)
}

func TestApprovalCheckpointRejectsUnsupportedContext(t *testing.T) {
	for _, selection := range []any{"wrong type", []schemas.BifrostContextKey{"unsupported"}} {
		t.Run(fmt.Sprint(selection), func(t *testing.T) {
			executor := newTestAsyncExecutor(t)
			ctx := schemas.NewBifrostContext(t.Context(), schemas.NoDeadline)
			ctx.SetValue(schemas.AgentCheckpointContextKeys, selection)
			ctx.SetValue(schemas.BifrostContextKey("unsupported"), make(chan bool))
			t.Cleanup(schemas.SetAgentApprovalState(ctx, &schemas.AgentApprovalState{}))
			job, err := executor.SubmitJob(ctx, 60, func(bg *schemas.BifrostContext) (any, *schemas.BifrostError) {
				schemas.GetAgentApprovalState(bg).Checkpoint = &schemas.AgentCheckpoint{}
				return &schemas.BifrostResponsesResponse{}, nil
			}, schemas.ResponsesRequest)
			require.NoError(t, err)
			require.Equal(t, schemas.AsyncJobStatusFailed, waitForJobStatus(t, executor.logstore, job.ID).Status)
			db, err := approvalDB(executor.logstore)
			require.NoError(t, err)
			var count int64
			require.NoError(t, db.Model(&AgentApproval{}).Where("job_id = ?", job.ID).Count(&count).Error)
			require.Zero(t, count)
		})
	}
}

func TestAgentApprovalStateReleased(t *testing.T) {
	for _, outcome := range []string{"completed", "failed", "panic", "awaiting_approval", "submission_failed"} {
		t.Run(outcome, func(t *testing.T) {
			executor := newTestAsyncExecutor(t)
			ctx := schemas.NewBifrostContext(t.Context(), schemas.NoDeadline)
			t.Cleanup(schemas.SetAgentApprovalState(ctx, &schemas.AgentApprovalState{}))
			if outcome == "submission_failed" {
				ctx.SetValue(schemas.BifrostContextKeyVirtualKey, "unknown-key")
			}
			job, err := executor.SubmitJob(ctx, 60, func(bg *schemas.BifrostContext) (any, *schemas.BifrostError) {
				switch outcome {
				case "panic":
					panic("test operation panic")
				case "failed":
					return nil, &schemas.BifrostError{Error: &schemas.ErrorField{Message: "test operation failure"}}
				case "awaiting_approval":
					schemas.GetAgentApprovalState(bg).Checkpoint = &schemas.AgentCheckpoint{
						Request: &schemas.BifrostResponsesRequest{}, Response: &schemas.BifrostResponsesResponse{},
						Pending: []schemas.ChatAssistantMessageToolCall{{ID: schemas.Ptr("call")}},
					}
				}
				return &schemas.BifrostResponsesResponse{}, nil
			}, schemas.ResponsesRequest)
			if outcome == "submission_failed" {
				require.Error(t, err)
				require.Nil(t, schemas.GetAgentApprovalState(ctx))
				return
			}
			require.NoError(t, err)
			want := schemas.AsyncJobStatus(outcome)
			if outcome == "panic" {
				want = schemas.AsyncJobStatusFailed
			}
			require.Eventually(t, func() bool {
				saved, err := executor.RetrieveJob(ctx, job.ID, nil, schemas.ResponsesRequest)
				return err == nil && saved.Status == want && schemas.GetAgentApprovalState(ctx) == nil
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}
