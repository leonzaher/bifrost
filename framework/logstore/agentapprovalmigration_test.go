package logstore

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestAsyncJobWebhookContextUpgrade(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(filepath.Join(t.TempDir(), "logstore.db")), &gorm.Config{})
	require.NoError(t, err)
	require.NoError(t, db.Exec(`CREATE TABLE async_jobs (id text PRIMARY KEY, status text NOT NULL, request_type text NOT NULL)`).Error)
	require.NoError(t, db.Exec(`INSERT INTO async_jobs (id, status, request_type) VALUES ('existing', 'pending', 'chat_completion')`).Error)
	require.False(t, db.Migrator().HasColumn(&AsyncJob{}, "webhook_context"))

	var step *migrationStep
	for i := range logstoreMigrationSteps {
		if len(logstoreMigrationSteps[i].IDs) == 1 && logstoreMigrationSteps[i].IDs[0] == "async_jobs_add_webhook_context_column" {
			step = &logstoreMigrationSteps[i]
			break
		}
	}
	require.NotNil(t, step, "existing databases need a registered webhook_context migration")
	require.NoError(t, step.run(context.Background(), db, testLogger{}))
	require.True(t, db.Migrator().HasColumn(&AsyncJob{}, "webhook_context"))
	require.NoError(t, db.Model(&AsyncJob{}).Where("id = ?", "existing").Update("webhook_context", `{"source":"upgrade"}`).Error)
	var job AsyncJob
	require.NoError(t, db.First(&job, "id = ?", "existing").Error)
	require.Equal(t, `{"source":"upgrade"}`, job.WebhookContext)
}
