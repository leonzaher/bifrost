package handlers

import (
	"errors"

	"github.com/bytedance/sonic"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/logstore"
	"github.com/maximhq/bifrost/transports/bifrost-http/lib"
	"github.com/valyala/fasthttp"
)

func (h *AsyncHandler) prepareToolApproval(ctx *fasthttp.RequestCtx, bifrostCtx *schemas.BifrostContext, req *schemas.BifrostResponsesRequest) bool {
	if string(ctx.Request.Header.Peek(schemas.AsyncToolApprovalHeader)) != "true" {
		return true
	}
	if h.client.MCPManager == nil {
		SendError(ctx, fasthttp.StatusBadRequest, "tool approval requires an MCP manager")
		return false
	}
	if err := h.executor.SupportsToolApproval(); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, err.Error())
		return false
	}
	if len(req.RawRequestBody) > 0 {
		SendError(ctx, fasthttp.StatusBadRequest, "tool approval requires structured Responses input")
		return false
	}
	state := &schemas.AgentApprovalState{}
	if value := ctx.Request.Header.Peek(schemas.AsyncAutoToolsHeader); len(value) > 0 {
		if err := sonic.Unmarshal(value, &state.AutoTools); err != nil || state.AutoTools == nil {
			SendError(ctx, fasthttp.StatusBadRequest, "x-bf-async-auto-tools must be a JSON array of exact tool names")
			return false
		}
	}
	schemas.SetAgentApprovalState(bifrostCtx, state)
	return true
}

func (h *AsyncHandler) approveTools(ctx *fasthttp.RequestCtx) {
	var input schemas.ToolApprovalDecision
	if err := sonic.Unmarshal(ctx.PostBody(), &input); err != nil {
		SendError(ctx, fasthttp.StatusBadRequest, "invalid approval decision")
		return
	}
	bifrostCtx, cancel := lib.ConvertToBifrostContext(ctx, h.handlerStore)
	if bifrostCtx == nil {
		SendError(ctx, fasthttp.StatusBadRequest, "failed to convert context")
		return
	}
	defer cancel()
	job, err := h.executor.ApproveAgent(bifrostCtx, input, h.client.ResumeResponsesRequest)
	if err != nil {
		status := fasthttp.StatusNotFound
		switch {
		case errors.Is(err, logstore.ErrApprovalConflict):
			status = fasthttp.StatusConflict
		case errors.Is(err, logstore.ErrInvalidApproval):
			status = fasthttp.StatusBadRequest
		case errors.Is(err, logstore.ErrJobInternal):
			status = fasthttp.StatusInternalServerError
		}
		SendError(ctx, status, err.Error())
		return
	}
	SendJSONWithStatus(ctx, job.ToResponse(), fasthttp.StatusAccepted)
}
