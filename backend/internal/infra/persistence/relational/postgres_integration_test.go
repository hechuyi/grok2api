package relational

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	"github.com/chenyme/grok2api/backend/internal/domain/media"
	"github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/repository"
)

type postgresConstraintState struct {
	OID        int64  `gorm:"column:oid"`
	Count      int64  `gorm:"column:constraint_count"`
	Definition string `gorm:"column:definition"`
}

func TestPostgresRepositoriesIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not configured")
	}
	ctx := context.Background()
	database, err := OpenPostgres(ctx, dsn, 10, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	var serverVersion int
	if err := database.db.WithContext(ctx).Raw("SELECT current_setting('server_version_num')::integer").Scan(&serverVersion).Error; err != nil {
		t.Fatal(err)
	}
	if serverVersion/10000 != 18 {
		t.Fatalf("PostgreSQL server_version_num = %d, want major version 18", serverVersion)
	}

	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	models := NewModelRepository(database)
	revisionAfterFirstInitialize, err := models.RuntimeRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	constraintAfterFirstInitialize := readPostgresMediaSecondsConstraint(t, ctx, database)
	if constraintAfterFirstInitialize.Count != 1 || constraintAfterFirstInitialize.OID == 0 {
		t.Fatalf("media seconds constraint after first initialize = %#v", constraintAfterFirstInitialize)
	}
	if postgresMediaSecondsConstraintNeedsMigration(constraintAfterFirstInitialize.Definition) {
		t.Fatalf("media seconds constraint is stale after first initialize: %q", constraintAfterFirstInitialize.Definition)
	}

	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatalf("second InitializeSchema: %v", err)
	}
	revisionAfterSecondInitialize, err := models.RuntimeRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revisionAfterSecondInitialize != revisionAfterFirstInitialize {
		t.Fatalf("runtime revision after idempotent initialize = %d, want %d", revisionAfterSecondInitialize, revisionAfterFirstInitialize)
	}
	constraintAfterSecondInitialize := readPostgresMediaSecondsConstraint(t, ctx, database)
	if constraintAfterSecondInitialize != constraintAfterFirstInitialize {
		t.Fatalf("media seconds constraint was replaced by second initialize: first=%#v second=%#v", constraintAfterFirstInitialize, constraintAfterSecondInitialize)
	}
	if !database.db.Migrator().HasIndex(&accountModelCapabilityModel{}, "idx_account_model_capabilities_upstream_account") {
		t.Fatal("missing reverse account capability index")
	}

	suffix := strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	accounts := NewAccountRepository(database)
	createdAccount, wasCreated, err := accounts.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, WebTier: account.WebTierAuto,
		Name: "postgres-web-" + suffix, SourceKey: "postgres-integration-" + suffix,
		EncryptedAccessToken: "encrypted", AuthStatus: account.AuthStatusActive,
	})
	if err != nil || !wasCreated || createdAccount.ID == 0 {
		t.Fatalf("account = %#v, created = %v, err = %v", createdAccount, wasCreated, err)
	}
	loadedAccount, err := accounts.Get(ctx, createdAccount.ID)
	if err != nil || loadedAccount.SourceKey != createdAccount.SourceKey || loadedAccount.Provider != account.ProviderWeb || loadedAccount.AuthType != account.AuthTypeSSO {
		t.Fatalf("loaded account = %#v, err = %v", loadedAccount, err)
	}
	revisionAfterAccount, err := models.RuntimeRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revisionAfterAccount != revisionAfterSecondInitialize+1 {
		t.Fatalf("runtime revision after account write = %d, want %d", revisionAfterAccount, revisionAfterSecondInitialize+1)
	}

	upstreamModel := "postgres-video-" + suffix
	capabilitySyncedAt := time.Now().UTC()
	if err := models.ReplaceAccountCapabilities(ctx, createdAccount.ID, []string{upstreamModel}, capabilitySyncedAt); err != nil {
		t.Fatal(err)
	}
	if synced, err := models.HasSuccessfulAccountSync(ctx, createdAccount.ID); err != nil || !synced {
		t.Fatalf("account capability sync state = %v, err = %v", synced, err)
	}
	var capabilityRows int64
	if err := database.db.WithContext(ctx).Model(&accountModelCapabilityModel{}).
		Where("account_id = ? AND upstream_model = ?", createdAccount.ID, upstreamModel).
		Count(&capabilityRows).Error; err != nil {
		t.Fatal(err)
	}
	if capabilityRows != 1 {
		t.Fatalf("account capability rows = %d, want 1", capabilityRows)
	}
	revisionAfterCapability, err := models.RuntimeRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revisionAfterCapability != revisionAfterAccount+1 {
		t.Fatalf("runtime revision after capability write = %d, want %d", revisionAfterCapability, revisionAfterAccount+1)
	}

	publicModel := "postgres-video-" + suffix
	createdRoute, err := models.Create(ctx, model.Route{
		PublicID: publicModel, Provider: account.ProviderWeb, UpstreamModel: upstreamModel,
		Capability: model.CapabilityVideo, Origin: model.OriginManual, Enabled: true,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if createdRoute.ID == 0 || createdRoute.PublicID != publicModel || createdRoute.UpstreamModel != upstreamModel {
		t.Fatalf("created route = %#v", createdRoute)
	}
	loadedRoute, err := models.GetByPublicID(ctx, publicModel)
	if err != nil || loadedRoute.ID != createdRoute.ID {
		t.Fatalf("loaded available route = %#v, err = %v", loadedRoute, err)
	}
	routes, totalRoutes, err := models.List(ctx, repository.ModelListQuery{Page: repository.PageQuery{Search: publicModel, Limit: 10}})
	if err != nil || totalRoutes != 1 || len(routes) != 1 {
		t.Fatalf("model route list = %#v, total = %d, err = %v", routes, totalRoutes, err)
	}
	if routes[0].SupportedAccounts != 1 || routes[0].SyncedAccounts < 1 || routes[0].TotalAccounts < routes[0].SyncedAccounts || routes[0].LastSyncedAt == nil {
		t.Fatalf("model capability aggregates = %#v", routes[0])
	}
	revisionAfterRoute, err := models.RuntimeRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if revisionAfterRoute != revisionAfterCapability+1 {
		t.Fatalf("runtime revision after route write = %d, want %d", revisionAfterRoute, revisionAfterCapability+1)
	}

	keys := NewClientKeyRepository(database)
	createdKey, err := keys.Create(ctx, clientkey.Key{
		Name: "postgres-key-" + suffix, Prefix: "pg" + suffix,
		SecretHash: strings.Repeat("a", 64), EncryptedSecret: "encrypted", Enabled: true,
		RPMLimit: clientkey.DefaultRPMLimit, MaxConcurrent: clientkey.DefaultMaxConcurrent,
		AllowedModels: []uint64{createdRoute.ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	loadedKey, err := keys.Get(ctx, createdKey.ID)
	if err != nil || len(loadedKey.AllowedModels) != 1 || loadedKey.AllowedModels[0] != createdRoute.ID {
		t.Fatalf("loaded client key = %#v, err = %v", loadedKey, err)
	}

	mediaJobs := NewMediaJobRepository(database)
	now := time.Now().UTC()
	job := media.Job{
		ID: "pg-video20-" + suffix, RequestID: "pg-video20-" + suffix,
		ClientKeyID: createdKey.ID, ClientKeyName: createdKey.Name,
		AccountID: createdAccount.ID, AccountName: createdAccount.Name,
		Provider: string(account.ProviderWeb), Model: publicModel, ModelRouteID: createdRoute.ID, UpstreamModel: upstreamModel,
		Prompt: "PostgreSQL 18 media constraint", Seconds: 20, Size: "16:9", Quality: "720p",
		Status: media.StatusQueued, InputJSON: `{"protocol":"legacy_v2"}`, CreatedAt: now, UpdatedAt: now,
	}
	if err := mediaJobs.CreateMediaJob(ctx, job); err != nil {
		t.Fatalf("create 20-second media job: %v", err)
	}
	loadedJob, err := mediaJobs.GetMediaJob(ctx, job.ID, createdKey.ID)
	if err != nil || loadedJob.Seconds != 20 || loadedJob.ModelRouteID != createdRoute.ID || loadedJob.AccountID != createdAccount.ID {
		t.Fatalf("loaded media job = %#v, err = %v", loadedJob, err)
	}
	invalidJob := job
	invalidJob.ID = "pg-video21-" + suffix
	invalidJob.RequestID = invalidJob.ID
	invalidJob.Seconds = 21
	if err := mediaJobs.CreateMediaJob(ctx, invalidJob); err == nil {
		t.Fatal("21-second media job unexpectedly passed chk_media_jobs_seconds")
	}

	constraintAfterWrites := readPostgresMediaSecondsConstraint(t, ctx, database)
	if constraintAfterWrites != constraintAfterFirstInitialize {
		t.Fatalf("media seconds constraint changed during repository writes: initial=%#v final=%#v", constraintAfterFirstInitialize, constraintAfterWrites)
	}
}

func readPostgresMediaSecondsConstraint(t *testing.T, ctx context.Context, database *Database) postgresConstraintState {
	t.Helper()
	var state postgresConstraintState
	if err := database.db.WithContext(ctx).Raw(`
		SELECT COALESCE(MIN(oid::bigint), 0) AS oid,
			COUNT(*) AS constraint_count,
			COALESCE(MAX(pg_get_constraintdef(oid)), '') AS definition
		FROM pg_constraint
		WHERE conrelid = 'media_jobs'::regclass AND conname = 'chk_media_jobs_seconds'
	`).Scan(&state).Error; err != nil {
		t.Fatal(err)
	}
	return state
}
