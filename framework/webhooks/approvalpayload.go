package webhooks

import (
	"strings"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/tidwall/gjson"
)

// approvalJustification joins each call's _injected_justification argument.
func approvalJustification(calls []schemas.ChatAssistantMessageToolCall) string {
	justifications := make([]string, 0, len(calls))
	for _, call := range calls {
		justification := "No justification provided"
		if value := gjson.Get(call.Function.Arguments, "_injected_justification"); gjson.Valid(call.Function.Arguments) &&
			value.Type == gjson.String && strings.TrimSpace(value.Str) != "" {
			justification = strings.TrimSpace(value.Str)
		}
		if len(calls) > 1 && call.Function.Name != nil {
			justification = *call.Function.Name + ": " + justification
		}
		justifications = append(justifications, justification)
	}
	return strings.Join(justifications, "\n")
}
