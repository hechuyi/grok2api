package model

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	"github.com/chenyme/grok2api/backend/internal/domain/account"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/pkg/batch"
	"github.com/chenyme/grok2api/backend/internal/repository"
	"golang.org/x/sync/singleflight"
)

const defaultModelSyncWorkers = 25
const syncFailurePersistTimeout = 5 * time.Second
const defaultRuntimeRevisionPollInterval = time.Second

var (
	ErrInvalidFilter = errors.New("模型筛选条件无效")
	ErrInvalidInput  = errors.New("模型参数无效")
	ErrNotFound      = errors.New("模型不存在")
	ErrConflict      = errors.New("模型名称冲突")
)

type UpdateInput struct {
	PublicID   *string
	Enabled    *bool
	AccountIDs *[]uint64
}

type CreateInput struct {
	PublicID      string
	Provider      account.Provider
	UpstreamModel string
	Capability    modeldomain.Capability
	Enabled       bool
	AccountIDs    []uint64
}

type AccountOption struct {
	ID   uint64
	Name string
}

type ListFilter struct {
	Provider string
	Status   string
	Sort     repository.SortQuery
}

// Service 负责上游模型发现、内部来源路由与对外模型名称维护。
type Service struct {
	models                      repository.ModelRepository
	accounts                    repository.AccountRepository
	account                     *accountapp.Service
	providers                   *provider.Registry
	bulkPool                    *batch.Pool
	logger                      *slog.Logger
	syncAll                     singleflight.Group
	runtimeLoad                 singleflight.Group
	runtimeCatalog              atomic.Pointer[runtimeCatalog]
	runtimeRevisionCheckAt      atomic.Int64
	runtimeRevisionPollInterval time.Duration
}

type runtimeRevisionRepository interface {
	RuntimeRevision(ctx context.Context) (uint64, error)
}

type runtimeCatalog struct {
	revision   uint64
	routes     []modeldomain.Route
	byPublicID map[string]modeldomain.Route
}

func NewService(models repository.ModelRepository, accounts repository.AccountRepository, accountService *accountapp.Service, providers *provider.Registry) *Service {
	return &Service{
		models: models, accounts: accounts, account: accountService, providers: providers,
		bulkPool: batch.NewPool(defaultModelSyncWorkers), logger: slog.Default(),
		runtimeRevisionPollInterval: defaultRuntimeRevisionPollInterval,
	}
}

func (s *Service) SetBulkPool(pool *batch.Pool) {
	if pool != nil {
		s.bulkPool = pool
	}
}

func (s *Service) SetLogger(logger *slog.Logger) {
	if logger != nil {
		s.logger = logger
	}
}

func (s *Service) List(ctx context.Context, page, pageSize int, search string, filter ListFilter) ([]modeldomain.Route, int64, error) {
	page, pageSize = normalizePage(page, pageSize)
	if !validProviderFilter(filter.Provider) || !validModelFilter(filter.Status, "", "enabled", "disabled") || !repository.IsValidSort(filter.Sort, "publicId", "upstreamModel", "status", "provider", "accountSupport", "lastSyncedAt") {
		return nil, 0, ErrInvalidFilter
	}
	var enabled *bool
	if filter.Status != "" {
		value := filter.Status == "enabled"
		enabled = &value
	}
	return s.models.List(ctx, repository.ModelListQuery{Page: repository.PageQuery{Offset: (page - 1) * pageSize, Limit: pageSize, Search: search, Sort: filter.Sort}, Filter: repository.ModelListFilter{Provider: filter.Provider, Enabled: enabled}})
}

func validProviderFilter(value string) bool {
	return value == "" || account.Provider(value).IsValid()
}

func validModelFilter(value string, allowed ...string) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func (s *Service) ListEnabled(ctx context.Context) ([]modeldomain.Route, error) {
	catalog, err := s.loadRuntimeCatalog(ctx)
	if err != nil {
		return nil, err
	}
	values := cloneRoutes(catalog.routes)
	result := values[:0]
	for _, value := range values {
		externalID := modeldomain.ExternalPublicID(value.Provider, value.PublicID)
		if value.Provider != account.ProviderWeb || !IsLegacyV2OnlyModel(externalID) {
			result = append(result, value)
		}
	}
	return result, nil
}

func (s *Service) Get(ctx context.Context, id uint64) (modeldomain.Route, error) {
	return s.models.Get(ctx, id)
}

// ListLegacyV2Enabled preserves the v2 model order and only synthesizes the
// old public contract for legacy clients. Native v3 clients keep the canonical
// repository view.
func (s *Service) ListLegacyV2Enabled(ctx context.Context) ([]modeldomain.Route, error) {
	catalog, err := s.loadRuntimeCatalog(ctx)
	if err != nil {
		return nil, err
	}
	canonical := catalog.routes
	byPublicID := make(map[string]modeldomain.Route, len(canonical))
	for _, value := range canonical {
		byPublicID[value.PublicID] = value
	}
	result := make([]modeldomain.Route, 0, len(canonical)+len(legacyV2Models))
	for _, legacy := range legacyV2Models {
		routingID, _ := LegacyV2RoutingID(legacy.PublicID)
		target, ok := byPublicID[routingID]
		if !ok {
			target, ok = byPublicID[legacyV2CanonicalRoutingID(legacy)]
		}
		if !ok {
			continue
		}
		target.PublicID = legacy.PublicID
		target.Capability = legacy.Capability
		result = append(result, target)
	}
	return result, nil
}

// GetByPublicID 从不可变运行时目录读取；多实例通过数据库修订号在有限时间内收敛。
func (s *Service) GetByPublicID(ctx context.Context, publicID string) (modeldomain.Route, error) {
	catalog, err := s.loadRuntimeCatalog(ctx)
	if err != nil {
		return modeldomain.Route{}, err
	}
	if value, ok := runtimeRouteByPublicID(catalog, publicID); ok {
		return cloneRoute(value), nil
	}
	legacy, ok := legacyV2ModelsByPublicID[publicID]
	if !ok {
		return modeldomain.Route{}, repository.ErrNotFound
	}
	value, ok := catalog.byPublicID[legacyV2CanonicalRoutingID(legacy)]
	if !ok {
		return modeldomain.Route{}, repository.ErrNotFound
	}
	return cloneRoute(value), nil
}

func (s *Service) GetLegacyV2ByPublicID(ctx context.Context, publicID string) (modeldomain.Route, error) {
	legacy, ok := legacyV2ModelsByPublicID[publicID]
	if !ok {
		return modeldomain.Route{}, repository.ErrNotFound
	}
	catalog, err := s.loadRuntimeCatalog(ctx)
	if err != nil {
		return modeldomain.Route{}, err
	}
	routingID, _ := LegacyV2RoutingID(publicID)
	if value, exists := catalog.byPublicID[routingID]; exists {
		return cloneRoute(value), nil
	}
	value, exists := catalog.byPublicID[legacyV2CanonicalRoutingID(legacy)]
	if !exists {
		return modeldomain.Route{}, repository.ErrNotFound
	}
	return cloneRoute(value), nil
}

func runtimeRouteByPublicID(catalog *runtimeCatalog, publicID string) (modeldomain.Route, bool) {
	if value, ok := catalog.byPublicID[publicID]; ok {
		return value, true
	}
	for _, candidate := range modeldomain.PublicIDCandidates(publicID) {
		if value, ok := catalog.byPublicID[candidate]; ok {
			return value, true
		}
	}
	return modeldomain.Route{}, false
}

func (s *Service) loadRuntimeCatalog(ctx context.Context) (*runtimeCatalog, error) {
	current := s.runtimeCatalog.Load()
	revisions, versioned := s.models.(runtimeRevisionRepository)
	if current != nil {
		if !versioned {
			return current, nil
		}
		now := time.Now().UnixNano()
		if interval := s.runtimeRevisionPollInterval; interval > 0 && now < s.runtimeRevisionCheckAt.Load() {
			return current, nil
		}
	}

	loaded, err, _ := s.runtimeLoad.Do("runtime-catalog", func() (any, error) {
		current := s.runtimeCatalog.Load()
		if current != nil && !versioned {
			return current, nil
		}
		now := time.Now()
		if current != nil && s.runtimeRevisionPollInterval > 0 && now.UnixNano() < s.runtimeRevisionCheckAt.Load() {
			return current, nil
		}

		var revision uint64
		if versioned {
			value, err := revisions.RuntimeRevision(ctx)
			if err != nil {
				if current != nil {
					s.logger.Warn("model_runtime_revision_failed", "error", err)
					return current, nil
				}
				return nil, err
			}
			revision = value
			s.runtimeRevisionCheckAt.Store(now.Add(s.runtimeRevisionPollInterval).UnixNano())
			if current != nil && current.revision == revision {
				return current, nil
			}
		}

		for attempt := 0; attempt < 3; attempt++ {
			routes, err := s.models.ListEnabled(ctx)
			if err != nil {
				return nil, err
			}
			if versioned {
				latest, err := revisions.RuntimeRevision(ctx)
				if err != nil {
					return nil, err
				}
				if latest != revision {
					revision = latest
					continue
				}
			}
			next := newRuntimeCatalog(revision, routes)
			s.runtimeCatalog.Store(next)
			return next, nil
		}
		return nil, fmt.Errorf("模型运行时目录在加载期间持续变化")
	})
	if err != nil {
		return nil, err
	}
	return loaded.(*runtimeCatalog), nil
}

func newRuntimeCatalog(revision uint64, routes []modeldomain.Route) *runtimeCatalog {
	immutable := cloneRoutes(routes)
	byPublicID := make(map[string]modeldomain.Route, len(immutable))
	for _, route := range immutable {
		byPublicID[route.PublicID] = route
	}
	return &runtimeCatalog{revision: revision, routes: immutable, byPublicID: byPublicID}
}

func cloneRoutes(values []modeldomain.Route) []modeldomain.Route {
	result := make([]modeldomain.Route, len(values))
	for index, value := range values {
		result[index] = cloneRoute(value)
	}
	return result
}

func cloneRoute(value modeldomain.Route) modeldomain.Route {
	value.BoundAccountIDs = append([]uint64(nil), value.BoundAccountIDs...)
	if value.LastSyncedAt != nil {
		lastSyncedAt := *value.LastSyncedAt
		value.LastSyncedAt = &lastSyncedAt
	}
	return value
}

func (s *Service) GetByPublicIDCandidates(ctx context.Context, publicID string) ([]modeldomain.Route, error) {
	return s.models.GetByPublicIDCandidates(ctx, publicID)
}

func (s *Service) GetByProviderUpstream(ctx context.Context, providerValue account.Provider, upstreamModel string) (modeldomain.Route, error) {
	return s.models.GetByProviderUpstream(ctx, providerValue, upstreamModel)
}

func (s *Service) Create(ctx context.Context, input CreateInput) (modeldomain.Route, error) {
	publicID, validPublicID := modeldomain.NormalizePublicID(input.Provider, input.PublicID)
	if !validPublicID {
		return modeldomain.Route{}, invalidInput("publicId 不能为空、不能携带其他 Provider 前缀，且长度不能超过 255 个字符")
	}
	upstreamModel, validUpstreamModel := modeldomain.NormalizeUpstreamModel(input.Provider, input.UpstreamModel)
	if !validUpstreamModel {
		return modeldomain.Route{}, invalidInput("upstreamModel 必须属于所选 Provider 且长度为 1-255 个字符")
	}
	definition, err := s.validateProviderCapability(input.Provider, input.Capability)
	if err != nil {
		return modeldomain.Route{}, err
	}
	if definition.ModelCatalog == provider.ModelCatalogStatic && s.providers.QuotaMode(input.Provider, upstreamModel) == "" {
		return modeldomain.Route{}, invalidInput(fmt.Sprintf("%s 仅支持内置模型目录中的上游模型", definition.ModelNamespace))
	}
	accountIDs, err := s.validateBoundAccounts(ctx, input.Provider, input.AccountIDs)
	if err != nil {
		return modeldomain.Route{}, err
	}
	value := modeldomain.Route{
		PublicID: publicID, Provider: input.Provider, UpstreamModel: upstreamModel,
		Capability: input.Capability, Origin: modeldomain.OriginManual, Enabled: input.Enabled,
	}
	created, err := s.models.Create(ctx, value, accountIDs)
	if err != nil {
		return modeldomain.Route{}, mapRepositoryError(err)
	}
	s.invalidateRuntimeCatalog()
	return created, nil
}

func (s *Service) Update(ctx context.Context, id uint64, input UpdateInput) (modeldomain.Route, error) {
	value, err := s.models.Get(ctx, id)
	if err != nil {
		return modeldomain.Route{}, mapRepositoryError(err)
	}
	if input.PublicID != nil {
		publicID, ok := modeldomain.NormalizePublicID(value.Provider, *input.PublicID)
		if !ok {
			return modeldomain.Route{}, invalidInput("publicId 不能为空、不能携带其他 Provider 前缀，且长度不能超过 255 个字符")
		}
		value.PublicID = publicID
	}
	if input.Enabled != nil {
		value.Enabled = *input.Enabled
	}
	var accountIDs *[]uint64
	if input.AccountIDs != nil {
		validated, validateErr := s.validateBoundAccounts(ctx, value.Provider, *input.AccountIDs)
		if validateErr != nil {
			return modeldomain.Route{}, validateErr
		}
		accountIDs = &validated
	}
	updated, err := s.models.Update(ctx, value, accountIDs)
	if err != nil {
		return modeldomain.Route{}, mapRepositoryError(err)
	}
	s.invalidateRuntimeCatalog()
	return updated, nil
}

func (s *Service) Delete(ctx context.Context, id uint64) error {
	if id == 0 {
		return invalidInput("模型 ID 无效")
	}
	if err := s.models.Delete(ctx, id); err != nil {
		return mapRepositoryError(err)
	}
	s.invalidateRuntimeCatalog()
	return nil
}

func (s *Service) BatchDelete(ctx context.Context, ids []uint64) (int64, error) {
	values, err := normalizeBatchIDs(ids)
	if err != nil {
		return 0, err
	}
	deleted, err := s.models.DeleteMany(ctx, values)
	if err == nil && deleted > 0 {
		s.invalidateRuntimeCatalog()
	}
	return deleted, err
}

func (s *Service) ListBindableAccounts(ctx context.Context, providerValue account.Provider) ([]AccountOption, error) {
	if !providerValue.IsValid() {
		return nil, invalidInput("账号来源无效")
	}
	values, _, err := s.accounts.List(ctx, repository.AccountListQuery{
		Page:   repository.PageQuery{Offset: 0, Limit: 1000},
		Filter: repository.AccountListFilter{Provider: string(providerValue)},
	})
	if err != nil {
		return nil, err
	}
	result := make([]AccountOption, 0, len(values))
	for _, value := range values {
		result = append(result, AccountOption{ID: value.ID, Name: value.Name})
	}
	return result, nil
}

func (s *Service) validateProviderCapability(providerValue account.Provider, capability modeldomain.Capability) (provider.Definition, error) {
	if !providerValue.IsValid() || s.providers == nil {
		return provider.Definition{}, invalidInput("provider 无效")
	}
	definition, ok := s.providers.Definition(providerValue)
	if !ok {
		return provider.Definition{}, invalidInput("provider 未注册能力定义")
	}
	if !definition.SupportsModelCapability(capability) {
		return provider.Definition{}, invalidInput(fmt.Sprintf("%s 不支持 %s 能力", definition.ModelNamespace, capability))
	}
	return definition, nil
}

func (s *Service) validateBoundAccounts(ctx context.Context, providerValue account.Provider, ids []uint64) ([]uint64, error) {
	if len(ids) > 1000 {
		return nil, invalidInput("单个模型最多绑定 1000 个账号")
	}
	unique := make(map[uint64]struct{}, len(ids))
	result := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, invalidInput("绑定账号 ID 无效")
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		result = append(result, id)
	}
	if len(result) == 0 {
		return result, nil
	}
	values, _, err := s.accounts.List(ctx, repository.AccountListQuery{
		Page:   repository.PageQuery{Offset: 0, Limit: 1000},
		Filter: repository.AccountListFilter{Provider: string(providerValue)},
	})
	if err != nil {
		return nil, err
	}
	available := make(map[uint64]bool, len(values))
	for _, value := range values {
		available[value.ID] = true
	}
	for _, id := range result {
		if !available[id] {
			return nil, invalidInput(fmt.Sprintf("账号 %d 不存在或与模型来源不匹配", id))
		}
	}
	return result, nil
}

// BatchSetEnabled 批量更新模型路由启停状态。
func (s *Service) BatchSetEnabled(ctx context.Context, ids []uint64, enabled bool) (int64, error) {
	values, err := normalizeBatchIDs(ids)
	if err != nil {
		return 0, err
	}
	updated, err := s.models.UpdateManyEnabled(ctx, values, enabled)
	if err == nil && updated > 0 {
		s.invalidateRuntimeCatalog()
	}
	return updated, err
}

// Sync 从全部启用账号同步模型能力，并按 Provider 幂等更新公开路由表。
func (s *Service) Sync(ctx context.Context) (int, error) {
	result := s.syncAll.DoChan("all", func() (any, error) {
		return s.syncAllAccounts(ctx)
	})
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case value := <-result:
		if value.Err != nil {
			return 0, value.Err
		}
		return value.Val.(int), nil
	}
}

func (s *Service) syncAllAccounts(ctx context.Context) (int, error) {
	if s.providers == nil {
		return 0, fmt.Errorf("Provider 注册表未初始化")
	}
	providerValues := s.providers.Providers()
	if len(providerValues) == 0 {
		return 0, fmt.Errorf("没有已注册的 Provider")
	}
	credentials := make([]account.Credential, 0)
	for _, providerValue := range providerValues {
		values, err := s.accounts.ListEnabled(ctx, providerValue)
		if err != nil {
			return 0, err
		}
		credentials = append(credentials, values...)
	}
	if len(credentials) == 0 {
		return 0, fmt.Errorf("没有可用于模型同步的账号")
	}
	results, summary, runErr := batch.Map(ctx, credentials, batch.Options{Workers: s.bulkPool.Limit(), Pool: s.bulkPool}, func(workCtx context.Context, value account.Credential) ([]string, error) {
		adapter, ok := s.providers.Models(value.Provider)
		if !ok {
			return nil, fmt.Errorf("Provider %s 未注册模型同步能力", value.Provider)
		}
		return s.syncAccountCapabilities(workCtx, value, adapter)
	})
	pool := s.bulkPool.Snapshot()
	s.logger.Info("model_bulk_sync_completed", "total", summary.Total, "submitted", summary.Submitted, "succeeded", summary.Succeeded, "failed", summary.Failed, "panicked", summary.Panicked, "duration_ms", summary.Duration.Milliseconds(), "canceled", summary.Canceled, "pool_limit", pool.Limit, "pool_active", pool.Active, "pool_queued", pool.Queued, "pool_peak", pool.Peak, "error", runErr)
	if runErr != nil {
		return 0, runErr
	}

	uniqueModels := make(map[account.Provider]map[string]struct{}, len(providerValues))
	succeeded := 0
	var lastErr error
	for index, result := range results {
		if result.Err != nil {
			var panicErr *batch.PanicError
			if errors.As(result.Err, &panicErr) {
				s.logger.Error("model_sync_panicked", "account_id", credentials[index].ID, "error", panicErr, "stack", string(panicErr.Stack))
			}
			lastErr = result.Err
			continue
		}
		succeeded++
		providerModels := uniqueModels[credentials[index].Provider]
		if providerModels == nil {
			providerModels = make(map[string]struct{})
			uniqueModels[credentials[index].Provider] = providerModels
		}
		for _, value := range result.Value {
			value = strings.TrimSpace(value)
			if value != "" {
				providerModels[value] = struct{}{}
			}
		}
	}
	if succeeded == 0 {
		if lastErr != nil {
			return 0, lastErr
		}
		return 0, fmt.Errorf("没有账号成功同步模型")
	}
	syncedModels := 0
	for _, providerValue := range providerValues {
		providerModels := uniqueModels[providerValue]
		if len(providerModels) == 0 {
			continue
		}
		models := make([]string, 0, len(providerModels))
		for value := range providerModels {
			models = append(models, value)
		}
		if err := s.models.UpsertDiscovered(ctx, providerValue, models); err != nil {
			return 0, err
		}
		syncedModels += len(models)
	}
	s.invalidateRuntimeCatalog()
	return syncedModels, nil
}

// HasSuccessfulAccountSync 判断账号是否已有成功模型能力快照，不触发上游请求。
func (s *Service) HasSuccessfulAccountSync(ctx context.Context, accountID uint64) (bool, error) {
	return s.models.HasSuccessfulAccountSync(ctx, accountID)
}

// SyncAccount 只同步指定账号，并把该账号发现的模型合并到公开路由目录。
func (s *Service) SyncAccount(ctx context.Context, accountID uint64) (int, error) {
	credential, err := s.accounts.Get(ctx, accountID)
	if err != nil {
		return 0, err
	}
	adapter, ok := s.providers.Models(credential.Provider)
	if !ok {
		return 0, fmt.Errorf("Provider %s 未注册", credential.Provider)
	}
	models, err := s.syncAccountCapabilities(ctx, credential, adapter)
	if err != nil {
		return 0, err
	}
	if err := s.models.UpsertDiscovered(ctx, credential.Provider, models); err != nil {
		return 0, err
	}
	s.invalidateRuntimeCatalog()
	return len(models), nil
}

// SyncAccounts 使用共享同步池追赶指定账号的模型能力，不扩大为全量同步。
func (s *Service) SyncAccounts(ctx context.Context, accountIDs []uint64) (int, int, error) {
	ids, err := normalizeBatchIDs(accountIDs)
	if err != nil {
		return 0, 0, err
	}
	results, summary, runErr := batch.Map(ctx, ids, batch.Options{Workers: s.bulkPool.Limit(), Pool: s.bulkPool}, func(workCtx context.Context, id uint64) (int, error) {
		return s.SyncAccount(workCtx, id)
	})
	for index, result := range results {
		if result.Err == nil {
			continue
		}
		var panicErr *batch.PanicError
		if errors.As(result.Err, &panicErr) {
			s.logger.Error("model_startup_sync_panicked", "account_id", ids[index], "error", panicErr, "stack", string(panicErr.Stack))
		}
	}
	s.logger.Info("model_startup_sync_completed", "total", summary.Total, "succeeded", summary.Succeeded, "failed", summary.Failed, "canceled", summary.Canceled, "error", runErr)
	return summary.Succeeded, summary.Failed, runErr
}

func (s *Service) invalidateRuntimeCatalog() {
	s.runtimeRevisionCheckAt.Store(0)
}

func (s *Service) syncAccountCapabilities(ctx context.Context, value account.Credential, adapter provider.ModelCatalogAdapter) ([]string, error) {
	attemptedAt := time.Now().UTC()
	credential, err := s.account.EnsureCredential(ctx, value, false)
	if err != nil {
		s.markCapabilitySyncFailed(value.ID, attemptedAt, err)
		return nil, err
	}
	values, err := adapter.ListModels(ctx, credential)
	if err != nil {
		s.markCapabilitySyncFailed(credential.ID, attemptedAt, err)
		return nil, err
	}
	models := normalizeDiscoveredModels(values)
	if normalizer, ok := adapter.(provider.AccountModelCapabilityNormalizer); ok {
		var billing *account.Billing
		snapshot, billingErr := s.accounts.GetBilling(ctx, credential.ID)
		if billingErr == nil {
			billing = &snapshot
		} else if !errors.Is(billingErr, repository.ErrNotFound) {
			// Billing 不存在按 Unknown 处理；其他仓储错误保留失败语义。
			s.markCapabilitySyncFailed(credential.ID, attemptedAt, billingErr)
			return nil, billingErr
		}
		models = normalizeDiscoveredModels(normalizer.NormalizeAccountModelCapabilities(models, billing, credential))
	}
	if err := s.models.ReplaceAccountCapabilities(ctx, credential.ID, models, attemptedAt); err != nil {
		s.markCapabilitySyncFailed(credential.ID, attemptedAt, err)
		return nil, err
	}
	return models, nil
}

func normalizeDiscoveredModels(values []string) []string {
	unique := make(map[string]struct{}, len(values))
	models := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := unique[value]; exists {
			continue
		}
		unique[value] = struct{}{}
		models = append(models, value)
	}
	return models
}

// markCapabilitySyncFailed 使用独立短超时保存失败状态，避免请求取消后丢失账号能力诊断信息。
func (s *Service) markCapabilitySyncFailed(accountID uint64, attemptedAt time.Time, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), syncFailurePersistTimeout)
	defer cancel()
	_ = s.models.MarkAccountCapabilitySyncFailed(ctx, accountID, attemptedAt, cause.Error())
}

func normalizePage(page, pageSize int) (int, int) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	return page, pageSize
}

func normalizeBatchIDs(ids []uint64) ([]uint64, error) {
	if len(ids) == 0 {
		return nil, invalidInput("至少选择一个模型")
	}
	if len(ids) > 500 {
		return nil, invalidInput("单次最多处理 500 个模型")
	}
	seen := make(map[uint64]struct{}, len(ids))
	result := make([]uint64, 0, len(ids))
	for _, id := range ids {
		if id == 0 {
			return nil, invalidInput("模型 ID 无效")
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, id)
	}
	return result, nil
}

// invalidInput 为可安全返回给管理端的模型参数错误附加稳定语义。
func invalidInput(message string) error {
	return fmt.Errorf("%w: %s", ErrInvalidInput, message)
}

// mapRepositoryError 将仓储错误转换为模型应用错误。
func mapRepositoryError(err error) error {
	if errors.Is(err, repository.ErrNotFound) {
		return ErrNotFound
	}
	if errors.Is(err, repository.ErrConflict) {
		return ErrConflict
	}
	return err
}
