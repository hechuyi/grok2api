package gateway

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
)

const expectedSelectorWindowSize = 64

func TestSelectorAcquireBoundsConcurrencySnapshotWindow(t *testing.T) {
	selector, limiter, key := newHotPathSelector(256)
	lease, err := selector.Acquire(context.Background(), key.provider, key.upstreamModel, key.quotaMode, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	lease.Release()
	if limiter.maxBatch > expectedSelectorWindowSize {
		t.Fatalf("CurrentMany max batch = %d, want <= %d", limiter.maxBatch, expectedSelectorWindowSize)
	}
	if limiter.batchCalls != 1 {
		t.Fatalf("CurrentMany calls = %d, want 1 for an available first window", limiter.batchCalls)
	}
}

func TestSelectorAcquireContinuesAfterFullWindow(t *testing.T) {
	selector, limiter, key := newHotPathSelector(160)
	for id := uint64(1); id <= expectedSelectorWindowSize; id++ {
		limiter.current[fmt.Sprintf("account:%d", id)] = 1
	}
	lease, err := selector.Acquire(context.Background(), key.provider, key.upstreamModel, key.quotaMode, "", nil, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Credential.ID <= expectedSelectorWindowSize {
		t.Fatalf("selected full-window account %d", lease.Credential.ID)
	}
	if limiter.maxBatch > expectedSelectorWindowSize || limiter.batchCalls < 2 {
		t.Fatalf("CurrentMany calls=%d maxBatch=%d, want multiple bounded windows", limiter.batchCalls, limiter.maxBatch)
	}
}

func TestSelectorAcquirePinnedDoesNotReadFullConcurrencySnapshot(t *testing.T) {
	selector, limiter, key := newHotPathSelector(4096)
	lease, err := selector.AcquirePinned(context.Background(), key.provider, 4096, key.upstreamModel, key.quotaMode, true)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if lease.Credential.ID != 4096 || limiter.batchCalls != 0 || limiter.currentCalls != 0 {
		t.Fatalf("lease=%d batchCalls=%d currentCalls=%d", lease.Credential.ID, limiter.batchCalls, limiter.currentCalls)
	}
}

func TestSelectorConcurrentAcquireAndConsumeQuota(t *testing.T) {
	selector, _, key := newHotPathSelector(256)
	window := &account.QuotaWindow{AccountID: 1, Mode: key.quotaMode, Remaining: 100_000, Total: 100_000}
	selector.mu.Lock()
	snapshot := selector.candidates[key]
	snapshot.values[0].QuotaWindow = window
	selector.candidates[key] = snapshot
	selector.mu.Unlock()

	ctx := context.Background()
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 100 {
				lease, err := selector.Acquire(ctx, key.provider, key.upstreamModel, key.quotaMode, "", nil, false)
				if err == nil {
					lease.Release()
				}
			}
		}()
	}
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range 200 {
				selector.ConsumeQuota(key.provider, 1, key.quotaMode, 1)
			}
		}()
	}
	workers.Wait()
}

func newHotPathSelector(count int) (*Selector, *hotPathConcurrencyLimiter, candidateCacheKey) {
	limiter := &hotPathConcurrencyLimiter{current: make(map[string]int)}
	selector := NewSelector(nil, limiter, memory.NewStickyStore(), nil, time.Hour, time.Second, time.Minute)
	key := candidateCacheKey{provider: account.ProviderBuild, upstreamModel: "grok-hotpath", quotaMode: ""}
	values := make([]account.RoutingCandidate, count)
	for index := range values {
		values[index].Credential = account.Credential{
			ID: uint64(index + 1), Provider: key.provider, AuthStatus: account.AuthStatusActive,
			Enabled: true, Priority: 100, MaxConcurrent: 1,
		}
	}
	selector.candidates[key] = candidateSnapshot{values: values, expiresAt: time.Now().Add(time.Hour)}
	return selector, limiter, key
}

type hotPathConcurrencyLimiter struct {
	mu           sync.Mutex
	current      map[string]int
	batchCalls   int
	currentCalls int
	maxBatch     int
}

func (l *hotPathConcurrencyLimiter) Acquire(_ context.Context, key string, limit int) (func(), bool, error) {
	l.mu.Lock()
	if l.current[key] >= limit {
		l.mu.Unlock()
		return nil, false, nil
	}
	l.current[key]++
	l.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			l.current[key]--
			l.mu.Unlock()
		})
	}, true, nil
}

func (l *hotPathConcurrencyLimiter) Current(_ context.Context, key string) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.currentCalls++
	return l.current[key], nil
}

func (l *hotPathConcurrencyLimiter) CurrentMany(_ context.Context, keys []string) (map[string]int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.batchCalls++
	if len(keys) > l.maxBatch {
		l.maxBatch = len(keys)
	}
	result := make(map[string]int, len(keys))
	for _, key := range keys {
		result[key] = l.current[key]
	}
	return result, nil
}
