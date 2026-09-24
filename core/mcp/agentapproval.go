package mcp

import (
	"fmt"
	"strings"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/core/schemas"
)

func validateApprovalCalls(calls []schemas.ChatAssistantMessageToolCall) *schemas.BifrostError {
	seen := make(map[string]bool, len(calls))
	for _, call := range calls {
		if call.ID == nil || *call.ID == "" || seen[*call.ID] || call.Function.Name == nil {
			return &schemas.BifrostError{Error: &schemas.ErrorField{Message: "tool calls require unique IDs and names"}}
		}
		seen[*call.ID] = true
		arguments := call.Function.Arguments
		if !strings.HasPrefix(strings.TrimSpace(arguments), "{") || !sonic.Valid([]byte(arguments)) {
			return &schemas.BifrostError{Error: &schemas.ErrorField{Message: "tool arguments must be a JSON object"}}
		}
	}
	return nil
}

func appendApprovalFeedback(history []interface{}, calls []schemas.ChatAssistantMessageToolCall, approval *schemas.AgentApprovalState) []interface{} {
	for _, call := range calls {
		instructions := approval.FurtherInstructions[*call.ID]
		approved, decided := approval.Decisions[*call.ID]
		if !decided || approved || instructions == "" {
			continue
		}
		history = append(history, schemas.ResponsesMessage{
			Role: schemas.Ptr(schemas.ResponsesInputMessageRoleUser),
			Content: &schemas.ResponsesMessageContent{ContentStr: schemas.Ptr(
				fmt.Sprintf("Regarding rejected tool call %s (%s):\n%s", *call.ID, *call.Function.Name, instructions),
			)},
		})
	}
	return history
}
