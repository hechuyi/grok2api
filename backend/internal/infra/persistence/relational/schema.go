package relational

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm/clause"
)

var schemaModels = []any{
	&adminModel{},
	&adminSessionModel{},
	&accountModel{},
	&accountCredentialModel{},
	&accountProviderLinkModel{},
	&webAccountProfileModel{},
	&quotaWindowModel{},
	&billingModel{},
	&quotaRecoveryModel{},
	&modelRouteModel{},
	&modelRouteAccountModel{},
	&accountModelCapabilityModel{},
	&accountModelSyncStateModel{},
	&modelRuntimeStateModel{},
	&clientKeyModel{},
	&clientKeyModelPermission{},
	&billingReservationModel{},
	&requestAuditModel{},
	&responseOwnershipModel{},
	&webResponseStateModel{},
	&mediaJobModel{},
	&mediaAssetModel{},
	&runtimeSettingsModel{},
	&egressNodeModel{},
}

var schemaIndexes = []string{
	"CREATE INDEX IF NOT EXISTS idx_admin_sessions_admin_created ON admin_sessions(admin_id, created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_admin_sessions_expires ON admin_sessions(expires_at)",
	"CREATE INDEX IF NOT EXISTS idx_accounts_routing ON provider_accounts(provider, enabled, auth_status, priority DESC, id ASC)",
	"CREATE INDEX IF NOT EXISTS idx_accounts_created_id ON provider_accounts(created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_quota_windows_due ON account_quota_windows(remaining, reset_at, account_id)",
	"CREATE INDEX IF NOT EXISTS idx_model_routes_created_id ON model_routes(created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_model_routes_enabled ON model_routes(enabled, public_id, id)",
	"CREATE INDEX IF NOT EXISTS idx_model_route_accounts_account_route ON model_route_accounts(account_id, model_route_id)",
	"CREATE INDEX IF NOT EXISTS idx_account_model_capabilities_upstream_account ON account_model_capabilities(upstream_model, account_id)",
	"CREATE INDEX IF NOT EXISTS idx_client_keys_created_id ON client_keys(created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_client_keys_status ON client_keys(enabled, expires_at, created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_client_key_models_route_key ON client_key_models(model_route_id, client_key_id)",
	"CREATE INDEX IF NOT EXISTS idx_billing_reservations_expiry ON billing_reservations(expires_at, client_key_id)",
	"CREATE INDEX IF NOT EXISTS idx_egress_nodes_scope_health ON egress_nodes(scope, enabled, health DESC, id ASC)",
	"CREATE INDEX IF NOT EXISTS idx_audits_created_id ON request_audits(created_at DESC, id DESC)",
	"CREATE UNIQUE INDEX IF NOT EXISTS idx_audits_event_id ON request_audits(event_id) WHERE event_id <> ''",
	"CREATE INDEX IF NOT EXISTS idx_audits_account_created_id ON request_audits(account_id, created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_audits_status_created_id ON request_audits(status_code, created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_audits_streaming_created_id ON request_audits(streaming, created_at DESC, id DESC)",
	"CREATE INDEX IF NOT EXISTS idx_response_ownership_expires ON response_ownership(expires_at)",
	"CREATE INDEX IF NOT EXISTS idx_response_ownership_account ON response_ownership(account_id)",
	"CREATE INDEX IF NOT EXISTS idx_response_ownership_client_key ON response_ownership(client_key_id)",
	"CREATE INDEX IF NOT EXISTS idx_web_response_states_expires ON web_response_states(expires_at)",
	"CREATE INDEX IF NOT EXISTS idx_web_response_states_account ON web_response_states(account_id, created_at DESC)",
	"CREATE INDEX IF NOT EXISTS idx_media_jobs_client_created ON media_jobs(client_key_id, created_at DESC)",
	"CREATE INDEX IF NOT EXISTS idx_media_jobs_recovery ON media_jobs(status, lease_until, created_at, id)",
	"CREATE INDEX IF NOT EXISTS idx_media_jobs_usage_recovery ON media_jobs(status, usage_recorded_at, completed_at, id)",
	"CREATE INDEX IF NOT EXISTS idx_media_assets_created ON media_assets(created_at DESC, id)",
}

// InitializeSchema 以当前持久化模型作为首版数据库结构基线。
func (d *Database) InitializeSchema(ctx context.Context) error {
	db := d.db.WithContext(ctx)
	// all 作用域会让 Build 与 Web 共用 UA、健康度和冷却状态，升级时直接移除旧节点。
	if db.Migrator().HasTable(&egressNodeModel{}) {
		if err := db.Where("scope = ?", "all").Delete(&egressNodeModel{}).Error; err != nil {
			return fmt.Errorf("清理旧版所有域出口节点: %w", err)
		}
	}
	if err := d.widenMediaJobSecondsConstraint(ctx); err != nil {
		return fmt.Errorf("迁移视频任务时长约束: %w", err)
	}
	if err := db.AutoMigrate(schemaModels...); err != nil {
		return fmt.Errorf("初始化数据库表: %w", err)
	}
	for _, statement := range schemaIndexes {
		if err := db.Exec(statement).Error; err != nil {
			return fmt.Errorf("初始化数据库索引: %w", err)
		}
	}
	state := modelRuntimeStateModel{ID: 1, Revision: 1, UpdatedAt: time.Now().UTC()}
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&state).Error; err != nil {
		return fmt.Errorf("初始化模型运行时修订号: %w", err)
	}
	return nil
}

func (d *Database) widenMediaJobSecondsConstraint(ctx context.Context) error {
	db := d.db.WithContext(ctx)
	if !db.Migrator().HasTable(&mediaJobModel{}) {
		return nil
	}
	const constraint = "chk_media_jobs_seconds"
	switch d.dialect {
	case "sqlite":
		var ddl string
		if err := db.Raw("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'media_jobs'").Scan(&ddl).Error; err != nil {
			return err
		}
		if !strings.Contains(ddl, "BETWEEN 1 AND 15") {
			return nil
		}
		return d.rebuildSQLiteMediaJobsForSeconds(ctx, ddl)
	case "postgres":
		var definition string
		if err := db.Raw(`
			SELECT COALESCE((
				SELECT pg_get_constraintdef(oid)
				FROM pg_constraint
				WHERE conrelid = 'media_jobs'::regclass AND conname = ?
			), '')
		`, constraint).Scan(&definition).Error; err != nil {
			return err
		}
		if !postgresMediaSecondsConstraintNeedsMigration(definition) {
			return nil
		}
		if err := db.Exec(`ALTER TABLE media_jobs DROP CONSTRAINT IF EXISTS ` + constraint).Error; err != nil {
			return err
		}
		if err := db.Exec(`ALTER TABLE media_jobs ADD CONSTRAINT ` + constraint + ` CHECK (seconds BETWEEN 1 AND 20) NOT VALID`).Error; err != nil {
			return err
		}
		return db.Exec(`ALTER TABLE media_jobs VALIDATE CONSTRAINT ` + constraint).Error
	default:
		return nil
	}
}

func postgresMediaSecondsConstraintNeedsMigration(definition string) bool {
	normalized := strings.ToLower(definition)
	normalized = strings.NewReplacer(
		" ", "", "\n", "", "\r", "", "\t", "", `"`, "", "(", "", ")", "",
	).Replace(normalized)
	return !strings.Contains(normalized, "secondsbetween1and20") &&
		!(strings.Contains(normalized, "seconds>=1") && strings.Contains(normalized, "seconds<=20"))
}

func (d *Database) rebuildSQLiteMediaJobsForSeconds(ctx context.Context, ddl string) error {
	const temporaryTable = "media_jobs_seconds_migration"
	create := strings.Replace(ddl, "BETWEEN 1 AND 15", "BETWEEN 1 AND 20", 1)
	replaced := false
	for _, prefix := range []string{"CREATE TABLE `media_jobs`", `CREATE TABLE "media_jobs"`, "CREATE TABLE media_jobs"} {
		if strings.Contains(create, prefix) {
			create = strings.Replace(create, prefix, "CREATE TABLE `"+temporaryTable+"`", 1)
			replaced = true
			break
		}
	}
	if !replaced {
		return fmt.Errorf("无法识别 SQLite media_jobs 建表语句")
	}
	sqlDB, err := d.db.DB()
	if err != nil {
		return err
	}
	connection, err := sqlDB.Conn(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	if _, err := connection.ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		return err
	}
	defer connection.ExecContext(context.WithoutCancel(ctx), "PRAGMA foreign_keys = ON")
	transaction, err := connection.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	rollback := true
	defer func() {
		if rollback {
			_ = transaction.Rollback()
		}
	}()
	statements := []string{
		"DROP TABLE IF EXISTS `" + temporaryTable + "`",
		create,
		"INSERT INTO `" + temporaryTable + "` SELECT * FROM `media_jobs`",
		"DROP TABLE `media_jobs`",
		"ALTER TABLE `" + temporaryTable + "` RENAME TO `media_jobs`",
	}
	for _, statement := range statements {
		if _, err := transaction.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if err := transaction.Commit(); err != nil {
		return err
	}
	rollback = false
	return nil
}
