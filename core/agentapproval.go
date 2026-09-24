package bifrost

import "github.com/maximhq/bifrost/core/schemas"

// ResumeResponsesRequest executes a saved response through the normal MCP loop.
func (bifrost *Bifrost) ResumeResponsesRequest(
	ctx *schemas.BifrostContext,
	checkpoint *schemas.AgentCheckpoint,
) (*schemas.BifrostResponsesResponse, *schemas.BifrostError) {
	if bifrost.MCPManager == nil || checkpoint == nil {
		return nil, &schemas.BifrostError{Error: &schemas.ErrorField{Message: "MCP manager and checkpoint are required"}}
	}
	return bifrost.MCPManager.CheckAndExecuteAgentForResponsesRequest(ctx, checkpoint.Request, checkpoint.Response, bifrost.makeResponsesRequest)
}
