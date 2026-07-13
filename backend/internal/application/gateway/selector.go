package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/repository"
	"golang.org/x/sync/singleflight"
)

type accountLease struct {
	Credential     account.Credential
	Billing        *account.Billing
	QuotaProbe     bool
	QuotaProbeKind account.QuotaRecoveryKind
	QuotaMode      string
	release        func()
}

const quotaProbeLease = 5 * time.Minute
const successPersistInterval = 30 * time.Second
const candidateCacheTTL = time.Second
const candidateSelectionWindow = 64

type candidateSnapshot struct {
	values         []account.RoutingCandidate
	groups         []candidateGroup
	byID           map[uint64]int
	quotaOverrides *sync.Map
	expiresAt      time.Time
}

type candidateGroup struct{ start, end int }

type candidateCacheKey struct {
	provider      account.Provider
	upstreamModel string
	quotaMode     string
}

type selectionCursorKey struct {
	cache candidateCacheKey
	group int
	kind  uint8
}

const (
	selectionNormal uint8 = iota
	selectionProbe
)

func (l *accountLease) Release() {
	if l != nil && l.release != nil {
		l.release()
		l.release = nil
	}
}

// Selector 实现可替换的 balanced 账号选择策略。
type Selector struct {
	accounts         repository.AccountRepository
	concurrency      repository.ConcurrencyLimiter
	sticky           repository.StickySessionRepository
	stickyTTL        time.Duration
	cooldownBase     time.Duration
	cooldownMax      time.Duration
	mu               sync.Mutex
	lastSelectedAt   map[uint64]time.Time
	lastSuccessAt    map[uint64]time.Time
	tieBreakSeed     uint64
	candidates       map[candidateCacheKey]candidateSnapshot
	selectionCursors map[selectionCursorKey]uint64
	candidateLoads   singleflight.Group
	tierOrders       interface {
		TierOrder(account.Provider, string) []account.WebTier
	}
}

func NewSelector(accounts repository.AccountRepository, concurrency repository.ConcurrencyLimiter, sticky repository.StickySessionRepository, tierOrders interface {
	TierOrder(account.Provider, string) []account.WebTier
}, stickyTTL, cooldownBase, cooldownMax time.Duration) *Selector {
	return &Selector{accounts: accounts, concurrency: concurrency, sticky: sticky, tierOrders: tierOrders, stickyTTL: stickyTTL, cooldownBase: cooldownBase, cooldownMax: cooldownMax, lastSelectedAt: make(map[uint64]time.Time), lastSuccessAt: make(map[uint64]time.Time), tieBreakSeed: rand.Uint64(), candidates: make(map[candidateCacheKey]candidateSnapshot), selectionCursors: make(map[selectionCursorKey]uint64)}
}

func (s *Selector) UpdateConfig(stickyTTL, cooldownBase, cooldownMax time.Duration) {
	s.mu.Lock()
	s.stickyTTL = stickyTTL
	s.cooldownBase = cooldownBase
	s.cooldownMax = cooldownMax
	s.mu.Unlock()
}

func (s *Selector) routingConfig() (time.Duration, time.Duration, time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stickyTTL, s.cooldownBase, s.cooldownMax
}

func (s *Selector) Acquire(ctx context.Context, provider account.Provider, upstreamModel, quotaMode, promptCacheKey string, excluded map[uint64]bool, allowQuotaProbe bool) (*accountLease, error) {
	now := time.Now().UTC()
	stickyKey := promptCacheStickyKey(promptCacheKey)
	cacheKey := candidateCacheKey{provider: provider, upstreamModel: upstreamModel, quotaMode: quotaMode}
	snapshot, err := s.loadCandidates(ctx, provider, upstreamModel, quotaMode, now)
	if err != nil {
		return nil, err
	}
	probeSeen := false
	if allowQuotaProbe {
		lease, seen, selectErr := s.selectCandidate(ctx, cacheKey, snapshot, now, excluded, selectionProbe)
		if selectErr != nil {
			return nil, selectErr
		}
		probeSeen = seen
		if lease != nil {
			return lease, nil
		}
	}
	if stickyKey != "" {
		stickyID, ok, err := s.sticky.Get(ctx, stickyKey, now)
		if err != nil {
			return nil, fmt.Errorf("读取会话粘滞状态: %w", err)
		}
		if ok {
			if index, exists := snapshot.byID[stickyID]; exists {
				candidate := snapshotCandidate(snapshot, index)
				if normalCandidateEligible(candidate, now, excluded) {
					lease, acquireErr := s.tryAcquire(ctx, candidate.Credential)
					if acquireErr != nil {
						return nil, acquireErr
					}
					if lease != nil {
						lease.Billing = candidate.Billing
						lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
						return lease, nil
					}
				}
			}
		}
	}
	lease, normalSeen, err := s.selectCandidate(ctx, cacheKey, snapshot, now, excluded, selectionNormal)
	if err != nil {
		return nil, err
	}
	if lease != nil {
		if stickyKey != "" {
			stickyTTL, _, _ := s.routingConfig()
			if err := s.sticky.Set(ctx, stickyKey, lease.Credential.ID, now.Add(stickyTTL)); err != nil {
				lease.Release()
				return nil, fmt.Errorf("写入会话粘滞状态: %w", err)
			}
		}
		return lease, nil
	}
	if !probeSeen && !normalSeen {
		return nil, fmt.Errorf("没有可用上游账号")
	}
	return nil, fmt.Errorf("所有上游账号均达到并发上限")
}

// promptCacheStickyKey 将调用方缓存键压缩为固定长度，仅用于本地账号粘滞索引。
func promptCacheStickyKey(value string) string {
	if value == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// AcquirePinned 为 previous_response_id 等账号归属请求获取指定账号租约。
func (s *Selector) AcquirePinned(ctx context.Context, provider account.Provider, accountID uint64, upstreamModel, quotaMode string, inference bool) (*accountLease, error) {
	now := time.Now().UTC()
	snapshot, err := s.loadCandidates(ctx, provider, upstreamModel, quotaMode, now)
	if err != nil {
		return nil, err
	}
	index, exists := snapshot.byID[accountID]
	if !exists {
		return nil, fmt.Errorf("绑定的上游账号不存在")
	}
	candidate := snapshotCandidate(snapshot, index)
	value := candidate.Credential
	if !value.Enabled || value.AuthStatus != account.AuthStatusActive {
		return nil, fmt.Errorf("绑定的上游账号不可用")
	}
	if inference {
		if candidate.ModelCapabilityKnown && !candidate.SupportsModel {
			return nil, fmt.Errorf("绑定的上游账号不支持该模型")
		}
		if value.CooldownUntil != nil && now.Before(*value.CooldownUntil) {
			return nil, fmt.Errorf("绑定的上游账号正在冷却")
		}
		if recovery := candidate.QuotaRecovery; recovery != nil && recovery.Status != account.QuotaRecoveryStatusActive {
			if recovery.NextProbeAt == nil || now.Before(*recovery.NextProbeAt) {
				return nil, fmt.Errorf("绑定的上游账号额度等待重置")
			}
			lease, err := s.tryAcquire(ctx, value)
			if err != nil {
				return nil, err
			}
			if lease == nil {
				return nil, fmt.Errorf("绑定的上游账号达到并发上限")
			}
			claimed, err := s.accounts.ClaimQuotaProbe(ctx, value.ID, now, now.Add(quotaProbeLease))
			if err != nil || !claimed {
				lease.Release()
				if err != nil {
					return nil, err
				}
				return nil, fmt.Errorf("绑定的上游账号恢复探测已被占用")
			}
			lease.QuotaProbe = true
			lease.QuotaProbeKind = recovery.Kind
			lease.Billing = candidate.Billing
			return lease, nil
		}
		if candidate.Billing != nil && candidate.Billing.IsExhausted(value.MinimumRemaining) {
			return nil, fmt.Errorf("绑定的上游账号额度不足")
		}
		if candidate.QuotaWindow != nil && candidate.QuotaWindow.Remaining <= 0 {
			return nil, fmt.Errorf("绑定的上游账号该模式额度等待重置")
		}
	}
	lease, err := s.tryAcquire(ctx, value)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("绑定的上游账号达到并发上限")
	}
	lease.Billing = candidate.Billing
	lease.QuotaMode = effectiveQuotaMode(candidate, quotaMode)
	return lease, nil
}

// selectCandidate scans immutable static-priority groups and reads dynamic
// concurrency only for bounded rotating windows. A full window never prevents
// later windows or lower static-priority groups from being considered.
func (s *Selector) selectCandidate(ctx context.Context, cacheKey candidateCacheKey, snapshot candidateSnapshot, now time.Time, excluded map[uint64]bool, kind uint8) (*accountLease, bool, error) {
	seen := false
	for groupIndex, group := range snapshot.groups {
		length := group.end - group.start
		if length <= 0 {
			continue
		}
		start := s.nextGroupOffset(cacheKey, groupIndex, kind, length)
		window := make([]account.RoutingCandidate, 0, min(candidateSelectionWindow, length))
		for scanned := 0; scanned < length; scanned++ {
			index := group.start + (start+scanned)%length
			candidate := snapshotCandidate(snapshot, index)
			eligible := normalCandidateEligible(candidate, now, excluded)
			if kind == selectionProbe {
				eligible = probeCandidateEligible(candidate, now, excluded)
			}
			if eligible {
				seen = true
				window = append(window, candidate)
			}
			if len(window) < candidateSelectionWindow && scanned+1 < length {
				continue
			}
			if len(window) == 0 {
				continue
			}
			ordered, err := s.rankCandidateWindow(ctx, window, now)
			if err != nil {
				return nil, seen, err
			}
			for _, ranked := range ordered {
				if ranked.full {
					continue
				}
				candidate := ranked.candidate
				lease, err := s.tryAcquire(ctx, candidate.Credential)
				if err != nil {
					return nil, seen, err
				}
				if lease == nil {
					continue
				}
				if kind == selectionProbe {
					claimed, claimErr := s.accounts.ClaimQuotaProbe(ctx, candidate.Credential.ID, now, now.Add(quotaProbeLease))
					if claimErr != nil || !claimed {
						lease.Release()
						if claimErr != nil {
							return nil, seen, claimErr
						}
						continue
					}
					lease.QuotaProbe = true
					lease.QuotaProbeKind = candidate.QuotaRecovery.Kind
				}
				lease.Billing = candidate.Billing
				lease.QuotaMode = effectiveQuotaMode(candidate, cacheKey.quotaMode)
				return lease, seen, nil
			}
			window = window[:0]
		}
	}
	return nil, seen, nil
}

type rankedCandidate struct {
	candidate account.RoutingCandidate
	inFlight  int
	full      bool
}

func (s *Selector) rankCandidateWindow(ctx context.Context, values []account.RoutingCandidate, now time.Time) ([]rankedCandidate, error) {
	keys := make([]string, len(values))
	for index, candidate := range values {
		keys[index] = fmt.Sprintf("account:%d", candidate.Credential.ID)
	}
	counts := make(map[string]int, len(keys))
	batchReader, batched := s.concurrency.(repository.ConcurrencySnapshotReader)
	if batched {
		var err error
		counts, err = batchReader.CurrentMany(ctx, keys)
		if err != nil {
			return nil, fmt.Errorf("批量读取账号并发租约: %w", err)
		}
	}
	ranked := make([]rankedCandidate, len(values))
	for index, candidate := range values {
		key := keys[index]
		current := counts[key]
		if !batched {
			var err error
			current, err = s.concurrency.Current(ctx, key)
			if err != nil {
				return nil, fmt.Errorf("读取账号并发租约: %w", err)
			}
		}
		limit := candidate.Credential.MaxConcurrent
		if limit <= 0 {
			limit = account.DefaultMaxConcurrent
		}
		ranked[index] = rankedCandidate{candidate: candidate, inFlight: current, full: current >= limit}
	}
	s.mu.Lock()
	lastSelected := make(map[uint64]time.Time, len(values))
	for _, candidate := range values {
		lastSelected[candidate.Credential.ID] = s.lastSelectedAt[candidate.Credential.ID]
	}
	s.mu.Unlock()
	sort.SliceStable(ranked, func(i, j int) bool {
		left, right := ranked[i], ranked[j]
		if left.full != right.full {
			return !left.full
		}
		leftFresh, rightFresh := billingFresh(left.candidate, now), billingFresh(right.candidate, now)
		if leftFresh != rightFresh {
			return leftFresh
		}
		if left.inFlight != right.inFlight {
			return left.inFlight < right.inFlight
		}
		leftRemaining, rightRemaining := billingRemaining(left.candidate), billingRemaining(right.candidate)
		if leftRemaining != rightRemaining {
			return leftRemaining > rightRemaining
		}
		leftID, rightID := left.candidate.Credential.ID, right.candidate.Credential.ID
		if !lastSelected[leftID].Equal(lastSelected[rightID]) {
			return lastSelected[leftID].Before(lastSelected[rightID])
		}
		leftRank, rightRank := accountTieBreakRank(leftID, s.tieBreakSeed), accountTieBreakRank(rightID, s.tieBreakSeed)
		if leftRank != rightRank {
			return leftRank < rightRank
		}
		return leftID < rightID
	})
	return ranked, nil
}

func billingFresh(candidate account.RoutingCandidate, now time.Time) bool {
	return candidate.Billing != nil && now.Sub(candidate.Billing.SyncedAt) <= 30*time.Minute
}

func billingRemaining(candidate account.RoutingCandidate) float64 {
	if candidate.Billing == nil {
		return 0
	}
	return candidate.Billing.Remaining()
}

func normalCandidateEligible(candidate account.RoutingCandidate, now time.Time, excluded map[uint64]bool) bool {
	value := candidate.Credential
	if excluded[value.ID] || value.AuthStatus != account.AuthStatusActive {
		return false
	}
	if candidate.ModelCapabilityKnown && !candidate.SupportsModel {
		return false
	}
	if value.CooldownUntil != nil && now.Before(*value.CooldownUntil) {
		return false
	}
	if candidate.QuotaRecovery != nil && candidate.QuotaRecovery.Status != account.QuotaRecoveryStatusActive {
		return false
	}
	if candidate.Billing != nil && candidate.Billing.IsExhausted(value.MinimumRemaining) {
		return false
	}
	return candidate.QuotaWindow == nil || candidate.QuotaWindow.Remaining > 0
}

func probeCandidateEligible(candidate account.RoutingCandidate, now time.Time, excluded map[uint64]bool) bool {
	value := candidate.Credential
	if excluded[value.ID] || value.AuthStatus != account.AuthStatusActive {
		return false
	}
	if candidate.ModelCapabilityKnown && !candidate.SupportsModel {
		return false
	}
	if value.CooldownUntil != nil && now.Before(*value.CooldownUntil) {
		return false
	}
	recovery := candidate.QuotaRecovery
	return recovery != nil && recovery.Status != account.QuotaRecoveryStatusActive && recovery.NextProbeAt != nil && !now.Before(*recovery.NextProbeAt)
}

func (s *Selector) nextGroupOffset(cache candidateCacheKey, group int, kind uint8, length int) int {
	key := selectionCursorKey{cache: cache, group: group, kind: kind}
	s.mu.Lock()
	if s.selectionCursors == nil {
		s.selectionCursors = make(map[selectionCursorKey]uint64)
	}
	value := s.selectionCursors[key]
	s.selectionCursors[key] = value + candidateSelectionWindow
	s.mu.Unlock()
	return int(value % uint64(length))
}

func effectiveQuotaMode(candidate account.RoutingCandidate, fallback string) string {
	if candidate.QuotaWindow != nil && candidate.QuotaWindow.Mode == "weekly" {
		return "weekly"
	}
	return fallback
}

func (s *Selector) MarkSuccess(ctx context.Context, credential account.Credential) {
	s.markSuccess(ctx, credential, true)
}

func (s *Selector) markSuccess(ctx context.Context, credential account.Credential, quotaProbe bool) {
	now := time.Now().UTC()
	persist := credential.FailureCount > 0 || credential.CooldownUntil != nil || credential.LastError != ""
	s.mu.Lock()
	if last := s.lastSuccessAt[credential.ID]; last.IsZero() || now.Sub(last) >= successPersistInterval {
		persist = true
	}
	if persist {
		s.lastSuccessAt[credential.ID] = now
	}
	s.mu.Unlock()
	if persist {
		_ = s.accounts.UpdateHealth(ctx, credential.ID, 0, nil, "", true)
	}
	if quotaProbe {
		_ = s.accounts.ClearQuotaRecovery(ctx, credential.ID)
	}
	if quotaProbe || credential.FailureCount > 0 || credential.CooldownUntil != nil || credential.LastError != "" {
		s.invalidateCandidates(credential.Provider)
	}
}

func (s *Selector) MarkFreeQuotaExhausted(ctx context.Context, credential account.Credential, used, limit int64) {
	now := time.Now().UTC()
	nextProbeAt := now.Add(24 * time.Hour)
	_ = s.accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
		AccountID: credential.ID, Kind: account.QuotaRecoveryKindFree, Status: account.QuotaRecoveryStatusExhausted,
		ConfirmedUsed: used, ConfirmedLimit: limit, ExhaustedAt: &now,
		NextProbeAt: &nextProbeAt, LastConfirmedAt: &now, UpdatedAt: now,
	})
	_ = s.sticky.DeleteByAccount(ctx, credential.ID)
	s.invalidateCandidates(credential.Provider)
}

// MarkPaidQuotaExhausted 使用已知真实账期将付费账号移出号池，到期后才允许 Billing 探测。
func (s *Selector) MarkPaidQuotaExhausted(ctx context.Context, credential account.Credential, billing *account.Billing) bool {
	if billing == nil || (billing.MonthlyLimit <= 0 && billing.OnDemandCap <= 0 && billing.OnDemandUsed <= 0 && billing.PrepaidBalance <= 0 && billing.CreditUsagePercent <= 0) {
		return false
	}
	periodEnd, ok := billing.PeriodEnd()
	if !ok {
		return false
	}
	now := time.Now().UTC()
	_ = s.accounts.SaveQuotaRecovery(ctx, account.QuotaRecovery{
		AccountID: credential.ID, Kind: account.QuotaRecoveryKindPaid, Status: account.QuotaRecoveryStatusExhausted,
		ExhaustedAt: &now, NextProbeAt: &periodEnd, LastConfirmedAt: &now, UpdatedAt: now,
	})
	_ = s.sticky.DeleteByAccount(ctx, credential.ID)
	s.invalidateCandidates(credential.Provider)
	return true
}

// MarkQuotaStateChanged 在 Billing 探测改变持久化额度状态后立即失效候选快照。
func (s *Selector) MarkQuotaStateChanged(provider account.Provider) { s.invalidateCandidates(provider) }

// ConsumeQuota 将成功请求的本地额度变化应用到候选快照，避免为单账号变化清空整个 Provider 缓存。
func (s *Selector) ConsumeQuota(provider account.Provider, accountID uint64, mode string, amount int) {
	if provider != account.ProviderWeb || accountID == 0 || mode == "" || mode == "weekly" || amount <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key, snapshot := range s.candidates {
		if key.provider != provider {
			continue
		}
		if snapshot.byID == nil {
			snapshot = buildCandidateSnapshot(snapshot.values, snapshot.expiresAt, s.resolveTierOrder(key.provider, key.upstreamModel))
			s.candidates[key] = snapshot
		}
		index, ok := snapshot.byID[accountID]
		if !ok {
			continue
		}
		candidate := snapshotCandidate(snapshot, index)
		if candidate.QuotaWindow == nil || candidate.QuotaWindow.Mode != mode {
			continue
		}
		window := *candidate.QuotaWindow
		window.Remaining = max(0, window.Remaining-amount)
		window.UpdatedAt = time.Now().UTC()
		snapshot.quotaOverrides.Store(accountID, window)
	}
}

func (s *Selector) MarkFailure(ctx context.Context, credential account.Credential, status int, retryAfter time.Duration) {
	failureCount := credential.FailureCount + 1
	_, cooldownBase, cooldownMax := s.routingConfig()
	cooldown := cooldownBase
	for i := 1; i < failureCount && cooldown < cooldownMax; i++ {
		cooldown *= 2
	}
	if cooldown > cooldownMax {
		cooldown = cooldownMax
	}
	if retryAfter > cooldown {
		cooldown = retryAfter
	}
	until := time.Now().UTC().Add(cooldown)
	_ = s.accounts.UpdateHealth(ctx, credential.ID, failureCount, &until, fmt.Sprintf("upstream status %d", status), false)
	s.invalidateCandidates(credential.Provider)
	if status == 401 || status == 402 || status == 403 || status == 429 {
		_ = s.sticky.DeleteByAccount(ctx, credential.ID)
	}
}

func (s *Selector) loadCandidates(ctx context.Context, provider account.Provider, upstreamModel, quotaMode string, now time.Time) (candidateSnapshot, error) {
	key := candidateCacheKey{provider: provider, upstreamModel: upstreamModel, quotaMode: quotaMode}
	s.mu.Lock()
	if snapshot, ok := s.candidates[key]; ok && now.Before(snapshot.expiresAt) {
		if snapshot.byID == nil {
			snapshot = buildCandidateSnapshot(snapshot.values, snapshot.expiresAt, s.resolveTierOrder(provider, upstreamModel))
			s.candidates[key] = snapshot
		}
		s.mu.Unlock()
		return snapshot, nil
	}
	s.mu.Unlock()
	loadKey := string(provider) + "\x00" + upstreamModel + "\x00" + quotaMode
	loaded, err, _ := s.candidateLoads.Do(loadKey, func() (any, error) {
		checkTime := time.Now().UTC()
		s.mu.Lock()
		if snapshot, ok := s.candidates[key]; ok && checkTime.Before(snapshot.expiresAt) {
			if snapshot.byID == nil {
				snapshot = buildCandidateSnapshot(snapshot.values, snapshot.expiresAt, s.resolveTierOrder(provider, upstreamModel))
				s.candidates[key] = snapshot
			}
			s.mu.Unlock()
			return snapshot, nil
		}
		s.mu.Unlock()
		values, err := s.accounts.ListRoutingCandidates(ctx, provider, upstreamModel, quotaMode)
		if err != nil {
			return candidateSnapshot{}, err
		}
		snapshot := buildCandidateSnapshot(values, checkTime.Add(candidateCacheTTL), s.resolveTierOrder(provider, upstreamModel))
		s.mu.Lock()
		s.candidates[key] = snapshot
		s.mu.Unlock()
		return snapshot, nil
	})
	if err != nil {
		return candidateSnapshot{}, err
	}
	return loaded.(candidateSnapshot), nil
}

func buildCandidateSnapshot(values []account.RoutingCandidate, expiresAt time.Time, tierOrder []account.WebTier) candidateSnapshot {
	type staticKey struct {
		supports bool
		known    bool
		tier     int
		priority int
	}
	buckets := make(map[staticKey][]account.RoutingCandidate, 16)
	keys := make([]staticKey, 0, 16)
	for _, candidate := range values {
		key := staticKey{
			supports: candidate.SupportsModel, known: candidate.ModelCapabilityKnown,
			tier: tierOrderRank(tierOrder, candidate.Credential.WebTier), priority: candidate.Credential.Priority,
		}
		if _, exists := buckets[key]; !exists {
			keys = append(keys, key)
		}
		buckets[key] = append(buckets[key], candidate)
	}
	sort.Slice(keys, func(i, j int) bool {
		left, right := keys[i], keys[j]
		if left.supports != right.supports {
			return left.supports
		}
		if left.known != right.known {
			return left.known
		}
		if left.tier != right.tier {
			return left.tier < right.tier
		}
		return left.priority > right.priority
	})
	ordered := make([]account.RoutingCandidate, 0, len(values))
	groups := make([]candidateGroup, 0, len(keys))
	for _, key := range keys {
		start := len(ordered)
		ordered = append(ordered, buckets[key]...)
		groups = append(groups, candidateGroup{start: start, end: len(ordered)})
	}
	byID := make(map[uint64]int, len(ordered))
	for index, candidate := range ordered {
		byID[candidate.Credential.ID] = index
	}
	return candidateSnapshot{values: ordered, groups: groups, byID: byID, quotaOverrides: &sync.Map{}, expiresAt: expiresAt}
}

func snapshotCandidate(snapshot candidateSnapshot, index int) account.RoutingCandidate {
	candidate := snapshot.values[index]
	if snapshot.quotaOverrides != nil {
		if value, ok := snapshot.quotaOverrides.Load(candidate.Credential.ID); ok {
			window := value.(account.QuotaWindow)
			candidate.QuotaWindow = &window
		}
	}
	return candidate
}

func (s *Selector) invalidateCandidates(provider account.Provider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range s.candidates {
		if key.provider == provider {
			delete(s.candidates, key)
		}
	}
	for key := range s.selectionCursors {
		if key.cache.provider == provider {
			delete(s.selectionCursors, key)
		}
	}
}

func (s *Selector) tryAcquire(ctx context.Context, value account.Credential) (*accountLease, error) {
	limit := value.MaxConcurrent
	if limit <= 0 {
		limit = account.DefaultMaxConcurrent
	}
	release, acquired, err := s.concurrency.Acquire(ctx, fmt.Sprintf("account:%d", value.ID), limit)
	if err != nil {
		return nil, fmt.Errorf("获取账号并发租约: %w", err)
	}
	if !acquired {
		return nil, nil
	}
	s.mu.Lock()
	s.lastSelectedAt[value.ID] = time.Now().UTC()
	s.mu.Unlock()
	return &accountLease{Credential: value, release: release}, nil
}

// accountTieBreakRank gives equivalent accounts a process-local stable random
// order. The selector still rotates selected accounts via lastSelectedAt, but
// a restart no longer concentrates the cold-start load on the lowest IDs.
func accountTieBreakRank(id, seed uint64) uint64 {
	value := id + seed + 0x9e3779b97f4a7c15
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return value ^ (value >> 31)
}

func (s *Selector) resolveTierOrder(provider account.Provider, upstreamModel string) []account.WebTier {
	if s.tierOrders == nil {
		return nil
	}
	return s.tierOrders.TierOrder(provider, upstreamModel)
}

func tierOrderRank(order []account.WebTier, tier account.WebTier) int {
	for index, value := range order {
		if value == tier {
			return index
		}
	}
	return len(order)
}
