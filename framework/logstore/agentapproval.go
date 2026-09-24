package logstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/migrator"
	"gorm.io/gorm"
)

var ErrApprovalConflict = errors.New("approval is no longer pending or has a different decision")
var ErrInvalidApproval = errors.New("approval must identify a saved tool call and include a decision")

// AgentApproval retains the immutable checkpoint and its decision as an audit record.
type AgentApproval struct {
	ID         string          `gorm:"primaryKey;type:varchar(36)"`
	JobID      string          `gorm:"index;type:varchar(255);not null"`
	Checkpoint string          `gorm:"type:text;not null"`
	ScopeHash  string          `gorm:"type:varchar(64);not null"`
	Decisions  map[string]bool `gorm:"type:text;serializer:json"`
	CreatedAt  time.Time
	DecidedAt  *time.Time

	FurtherInstructions map[string]string `gorm:"type:text;serializer:json"`
}

func (AgentApproval) TableName() string { return "async_agent_approvals" }

func migrationCreateAgentApprovals(ctx context.Context, db *gorm.DB, logger schemas.Logger) error {
	return migrator.New(db, migrator.DefaultOptions, []*migrator.Migration{{
		ID:      "async_agent_approvals_init",
		Migrate: func(tx *gorm.DB) error { return tx.WithContext(ctx).AutoMigrate(&AgentApproval{}) },
	}}).Migrate()
}

// approvalDB requires transactions to prevent duplicate approvals.
func approvalDB(store LogStore) (*gorm.DB, error) {
	switch s := store.(type) {
	case *RDBLogStore:
		return s.db, nil
	case *HybridLogStore:
		return approvalDB(s.inner)
	default:
		return nil, errors.New("tool approval requires a SQLite or PostgreSQL log store")
	}
}

func (e *AsyncJobExecutor) SupportsToolApproval() error {
	_, err := approvalDB(e.logstore)
	return err
}

// RetrieveJob retrieves a job by its ID, including any pending approval.
func (e *AsyncJobExecutor) RetrieveJob(ctx context.Context, jobID string, vkValue *string, operationType schemas.RequestType) (*AsyncJob, error) {
	job, err := e.retrieveJob(ctx, jobID, vkValue, operationType)
	if err != nil {
		return nil, err
	}
	if job.Status == schemas.AsyncJobStatusAwaitingApproval {
		job.Approval, err = FindPendingApproval(ctx, e.logstore, job.ID)
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrJobInternal, err)
		}
	}
	return job, nil
}

// Hash caller identity and forwarded headers without persisting credentials.
// Tool filters are restored from the checkpoint, not supplied by the approver.
func approvalScope(ctx *schemas.BifrostContext) (string, error) {
	values := []any{
		ctx.Value(schemas.BifrostContextKeyVirtualKey), ctx.Value(schemas.BifrostContextKeyUserID),
		ctx.Value(schemas.BifrostContextKeyMCPSessionID), ctx.Value(schemas.BifrostContextKeyMCPExtraHeaders),
	}
	data, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(data)), nil
}

func (e *AsyncJobExecutor) pauseAgent(ctx context.Context, job *AsyncJob, cp *schemas.AgentCheckpoint, scope string) error {
	cp.ContextValues = nil
	if selection := ctx.Value(schemas.AgentCheckpointContextKeys); selection != nil {
		keys, ok := selection.([]schemas.BifrostContextKey)
		if !ok {
			return errors.New("checkpoint context keys must be []BifrostContextKey")
		}
		cp.ContextValues = make(map[schemas.BifrostContextKey]any, len(keys))
		for _, key := range keys {
			cp.ContextValues[key] = ctx.Value(key)
		}
	}
	for key, target := range map[schemas.BifrostContextKey]*[]string{
		schemas.MCPContextKeyIncludeTools:   &cp.IncludeTools,
		schemas.MCPContextKeyIncludeClients: &cp.IncludeClients,
	} {
		if value := ctx.Value(key); value != nil {
			filters, ok := value.([]string)
			if !ok {
				return errors.New("invalid MCP tool filter")
			}
			*target = append([]string{}, filters...)
		}
	}
	db, err := approvalDB(e.logstore)
	if err != nil {
		return err
	}
	checkpoint, err := json.Marshal(cp)
	if err != nil {
		return err
	}
	response, err := json.Marshal(cp.Response)
	if err != nil {
		return err
	}
	record := &AgentApproval{
		ID: uuid.NewString(), JobID: job.ID, Checkpoint: string(checkpoint),
		ScopeHash: scope, CreatedAt: time.Now().UTC(),
	}
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(record).Error; err != nil {
			return err
		}
		result := tx.Model(&AsyncJob{}).Where("id = ? AND status = ?", job.ID, schemas.AsyncJobStatusProcessing).
			Updates(map[string]any{"status": schemas.AsyncJobStatusAwaitingApproval, "response": string(response), "status_code": 200})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrApprovalConflict
		}
		return nil
	})
}

// FindPendingApproval returns the latest saved approval batch for a job.
func FindPendingApproval(ctx context.Context, store LogStore, jobID string) (*schemas.ToolApprovalRequest, error) {
	db, err := approvalDB(store)
	if err != nil {
		return nil, err
	}
	var record AgentApproval
	// A decision may commit after the caller reads the job status. Return that
	// immutable record rather than turning an ordinary stale poll into a 500.
	if err := db.WithContext(ctx).Where("job_id = ?", jobID).Order("created_at DESC").First(&record).Error; err != nil {
		return nil, err
	}
	var cp schemas.AgentCheckpoint
	if err := json.Unmarshal([]byte(record.Checkpoint), &cp); err != nil {
		return nil, err
	}
	return &schemas.ToolApprovalRequest{ID: record.ID, Calls: cp.Pending}, nil
}

// ApproveAgent validates ownership and commits the decision before starting any tools.
func (e *AsyncJobExecutor) ApproveAgent(
	ctx *schemas.BifrostContext,
	input schemas.ToolApprovalDecision,
	resume func(*schemas.BifrostContext, *schemas.AgentCheckpoint) (*schemas.BifrostResponsesResponse, *schemas.BifrostError),
) (*AsyncJob, error) {
	if input.JobID == "" || input.RequestID == "" || input.ToolCallID == "" || input.Approved == nil {
		return nil, ErrInvalidApproval
	}
	input.FurtherInstructions = strings.TrimSpace(input.FurtherInstructions)
	if *input.Approved && input.FurtherInstructions != "" {
		return nil, fmt.Errorf("%w: further_instructions requires approved=false", ErrInvalidApproval)
	}
	job, err := e.retrieveJob(ctx, input.JobID, getVirtualKeyFromContext(ctx), schemas.ResponsesRequest)
	if err != nil {
		return nil, err
	}
	db, err := approvalDB(e.logstore)
	if err != nil {
		return nil, err
	}
	scope, err := approvalScope(ctx)
	if err != nil {
		return nil, err
	}
	var record AgentApproval
	var cp schemas.AgentCheckpoint
	var shouldResume bool
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// Take the write lock before reading on both SQLite and PostgreSQL.
		result := tx.Model(&AgentApproval{}).Where("id = ? AND job_id = ?", input.RequestID, job.ID).
			UpdateColumn("id", gorm.Expr("id"))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrApprovalConflict
		}
		if err := tx.First(&record, "id = ?", input.RequestID).Error; err != nil {
			return err
		}
		if scope != record.ScopeHash {
			return errors.New("approval execution scope does not match the original request")
		}
		decoder := json.NewDecoder(strings.NewReader(record.Checkpoint))
		decoder.UseNumber()
		if err := decoder.Decode(&cp); err != nil {
			return err
		}
		if cp.Request == nil || cp.Response == nil {
			return ErrInvalidApproval
		}
		if !slices.ContainsFunc(cp.Pending, func(call schemas.ChatAssistantMessageToolCall) bool {
			return call.ID != nil && *call.ID == input.ToolCallID
		}) {
			return ErrInvalidApproval
		}
		if record.Decisions == nil {
			record.Decisions = make(map[string]bool)
		}
		if approved, exists := record.Decisions[input.ToolCallID]; exists {
			if approved != *input.Approved || record.FurtherInstructions[input.ToolCallID] != input.FurtherInstructions {
				return ErrApprovalConflict
			}
			return nil
		}
		if record.DecidedAt != nil || job.Status != schemas.AsyncJobStatusAwaitingApproval {
			return ErrApprovalConflict
		}
		record.Decisions[input.ToolCallID] = *input.Approved
		if input.FurtherInstructions != "" {
			if record.FurtherInstructions == nil {
				record.FurtherInstructions = make(map[string]string)
			}
			record.FurtherInstructions[input.ToolCallID] = input.FurtherInstructions
		}
		if len(record.Decisions) == len(cp.Pending) {
			result := tx.Model(&AsyncJob{}).Where("id = ? AND status = ?", job.ID, schemas.AsyncJobStatusAwaitingApproval).
				Update("status", schemas.AsyncJobStatusProcessing)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrApprovalConflict
			}
			record.DecidedAt = schemas.Ptr(time.Now().UTC())
			shouldResume = true
		}
		return tx.Model(&record).Select("Decisions", "FurtherInstructions", "DecidedAt").Updates(&record).Error
	})
	if err != nil {
		return nil, err
	}
	if !shouldResume {
		if record.DecidedAt == nil {
			job.Approval = &schemas.ToolApprovalRequest{ID: record.ID, Calls: cp.Pending}
		}
		return job, nil
	}
	values := ctx.GetUserValues()
	keys := make([]schemas.BifrostContextKey, 0, len(cp.ContextValues))
	for key, value := range cp.ContextValues {
		values[key] = value
		keys = append(keys, key)
	}
	values[schemas.AgentCheckpointContextKeys] = keys
	for key, filters := range map[schemas.BifrostContextKey][]string{
		schemas.MCPContextKeyIncludeTools:   cp.IncludeTools,
		schemas.MCPContextKeyIncludeClients: cp.IncludeClients,
	} {
		delete(values, key)
		if filters != nil {
			values[key] = filters
		}
	}
	values[schemas.BifrostContextKeyRequestID] = uuid.NewString()
	approvalCtx := schemas.NewBifrostContextWithValue(context.Background(), schemas.NoDeadline, schemas.BifrostContextKeyRequestID, values[schemas.BifrostContextKeyRequestID])
	schemas.SetAgentApprovalState(approvalCtx, &schemas.AgentApprovalState{
		Decisions: record.Decisions, Depth: cp.Depth, MaxDepth: cp.MaxDepth, AutoTools: cp.AutoTools,
		FurtherInstructions: record.FurtherInstructions,
	})
	values[schemas.AgentApprovalContextKey] = approvalCtx.Value(schemas.AgentApprovalContextKey)
	job.Status = schemas.AsyncJobStatusProcessing
	job.Approval = nil
	go e.executeJob(job, func(bgCtx *schemas.BifrostContext) (any, *schemas.BifrostError) {
		return resume(bgCtx, &cp)
	}, values, ctx.Grant())
	return job, nil
}
