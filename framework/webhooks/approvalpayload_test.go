package webhooks

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRenderPayloadApproval(t *testing.T) {
	job := testAsyncJob()
	job.Status = schemas.AsyncJobStatusAwaitingApproval
	calls := []schemas.ChatAssistantMessageToolCall{{ID: schemas.Ptr("call"), Function: schemas.ChatAssistantMessageToolCallFunction{
		Name: schemas.Ptr("create_task"), Arguments: `{"id":9007199254740993,"_injected_justification":"  Create the task.  "}`,
	}}}
	job.Approval = &schemas.ToolApprovalRequest{ID: "approval", Calls: calls}
	body, err := renderPayload(job, tables.WebhookEventAsyncJobAwaitingApproval, true, 256*1024, time.Now())
	require.NoError(t, err)
	var payload struct {
		Data struct {
			Status        string `json:"status"`
			Justification string `json:"justification"`
			Response      any    `json:"response"`
			schemas.ToolApprovalRequest
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(body, &payload))
	assert.Equal(t, "awaiting_approval", payload.Data.Status)
	assert.Equal(t, job.Approval.ID, payload.Data.ID)
	assert.Equal(t, calls, payload.Data.Calls, "arguments must keep their precision")
	assert.Equal(t, "Create the task.", payload.Data.Justification)
	assert.Nil(t, payload.Data.Response, "approval events never inline a result")

	// A delayed approval event for a finished job carries no batch or result.
	job.Status, job.Approval = schemas.AsyncJobStatusCompleted, nil
	body, err = renderPayload(job, tables.WebhookEventAsyncJobAwaitingApproval, true, 256*1024, time.Now())
	require.NoError(t, err)
	data := decodeEnvelope(t, body)["data"].(map[string]any)
	assert.Equal(t, "awaiting_approval", data["status"])
	assert.NotContains(t, data, "calls")
	assert.NotContains(t, data, "response")
}

func TestApprovalJustification(t *testing.T) {
	call := func(arguments string) schemas.ChatAssistantMessageToolCall {
		return schemas.ChatAssistantMessageToolCall{Function: schemas.ChatAssistantMessageToolCallFunction{
			Name: schemas.Ptr("create_task"), Arguments: arguments,
		}}
	}
	for arguments, want := range map[string]string{
		`{"_injected_justification":" Create a task. "}`: "Create a task.",
		`{}`:                                   "No justification provided",
		`{"_injected_justification":" \n "}`:   "No justification provided",
		`{"_injected_justification":42}`:       "No justification provided",
		`{"_injected_justification":"partial"`: "No justification provided",
	} {
		assert.Equal(t, want, approvalJustification([]schemas.ChatAssistantMessageToolCall{call(arguments)}), arguments)
	}
	assert.Equal(t, "create_task: Create a task.\ncreate_task: No justification provided",
		approvalJustification([]schemas.ChatAssistantMessageToolCall{call(`{"_injected_justification":"Create a task."}`), call(`{}`)}))
}
