package logstore

import (
	"context"

	"github.com/maximhq/bifrost/core/schemas"
	"github.com/maximhq/bifrost/framework/migrator"
	"gorm.io/gorm"
)

func migrationAddAsyncJobWebhookContextColumn(ctx context.Context, db *gorm.DB, logger schemas.Logger) error {
	return migrator.New(db, migrator.DefaultOptions, []*migrator.Migration{{
		ID: "async_jobs_add_webhook_context_column",
		Migrate: func(tx *gorm.DB) error {
			return addColumnIfNotExists(tx.WithContext(ctx), logger, &AsyncJob{}, "webhook_context")
		},
	}}).Migrate()
}
