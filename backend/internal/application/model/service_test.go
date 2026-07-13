package model

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	accountapp "github.com/chenyme/grok2api/backend/internal/application/account"
	"github.com/chenyme/grok2api/backend/internal/domain/account"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/runtime/memory"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
)

func TestLegacyV2ModelsResolveAndListInRegistryOrderWithoutChangingNativeList(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "legacy-model-alias.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}

	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	credential, _, err := accountRepo.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, WebTier: account.WebTierBasic,
		Name: "legacy-route-account", SourceKey: "legacy-route-account", EncryptedAccessToken: "encrypted",
		ExpiresAt: time.Now().Add(time.Hour), Enabled: true, AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	canonical := []struct {
		publicID   string
		upstream   string
		capability modeldomain.Capability
	}{
		{publicID: "grok-chat-fast", upstream: "grok-chat-fast", capability: modeldomain.CapabilityChat},
		{publicID: "grok-chat-auto", upstream: "grok-chat-auto", capability: modeldomain.CapabilityChat},
		{publicID: "grok-chat-expert", upstream: "grok-chat-expert", capability: modeldomain.CapabilityChat},
		{publicID: "grok-chat-heavy", upstream: "grok-chat-heavy", capability: modeldomain.CapabilityChat},
		{publicID: "grok-imagine-image", upstream: "grok-imagine-image", capability: modeldomain.CapabilityImage},
		{publicID: "grok-imagine-image-quality", upstream: "grok-imagine-image-quality", capability: modeldomain.CapabilityImage},
		{publicID: "grok-imagine-image-edit", upstream: "imagine-image-edit", capability: modeldomain.CapabilityImageEdit},
		{publicID: "grok-imagine-video", upstream: "grok-imagine-video", capability: modeldomain.CapabilityVideo},
		{publicID: "grok-4.3-console", upstream: "grok-4.3-console", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.3-low", upstream: "grok-4.3-low", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.3-medium", upstream: "grok-4.3-medium", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.3-high", upstream: "grok-4.3-high", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-0309-reasoning-console", upstream: "grok-4.20-0309-reasoning-console", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-0309-console", upstream: "grok-4.20-0309-console", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-multi-agent-console", upstream: "grok-4.20-multi-agent-console", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-multi-agent-low", upstream: "grok-4.20-multi-agent-low", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-multi-agent-medium", upstream: "grok-4.20-multi-agent-medium", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-multi-agent-high", upstream: "grok-4.20-multi-agent-high", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-multi-agent-xhigh", upstream: "grok-4.20-multi-agent-xhigh", capability: modeldomain.CapabilityChat},
		{publicID: "grok-4.20-0309-non-reasoning-console", upstream: "grok-4.20-0309-non-reasoning-console", capability: modeldomain.CapabilityChat},
		{publicID: "grok-build-console", upstream: "grok-build-console", capability: modeldomain.CapabilityChat},
	}
	for _, value := range canonical {
		if _, err = modelRepo.Create(ctx, modeldomain.Route{
			PublicID: value.publicID, Provider: account.ProviderWeb, UpstreamModel: value.upstream,
			Capability: value.capability, Origin: modeldomain.OriginCatalog, Enabled: true,
		}, []uint64{credential.ID}); err != nil {
			t.Fatal(err)
		}
	}

	service := NewService(modelRepo, accountRepo, nil, nil)
	expectedIDs := []string{
		"grok-4.20-0309-non-reasoning", "grok-4.20-0309", "grok-4.20-0309-reasoning",
		"grok-4.20-0309-non-reasoning-super", "grok-4.20-0309-super", "grok-4.20-0309-reasoning-super",
		"grok-4.20-0309-non-reasoning-heavy", "grok-4.20-0309-heavy", "grok-4.20-0309-reasoning-heavy",
		"grok-4.20-multi-agent-0309", "grok-4.20-fast", "grok-4.3-fast", "grok-4.20-auto",
		"grok-4.20-expert", "grok-4.20-heavy", "grok-4.3-beta", "grok-imagine-image-lite",
		"grok-imagine-image", "grok-imagine-image-pro", "grok-imagine-image-edit", "grok-imagine-video",
		"grok-4.3-console", "grok-4.3-low", "grok-4.3-medium", "grok-4.3-high",
		"grok-4.20-0309-reasoning-console", "grok-4.20-0309-console", "grok-4.20-multi-agent-console",
		"grok-4.20-multi-agent-low", "grok-4.20-multi-agent-medium", "grok-4.20-multi-agent-high",
		"grok-4.20-multi-agent-xhigh", "grok-4.20-0309-non-reasoning-console", "grok-build-console",
	}
	if len(expectedIDs) != 34 {
		t.Fatalf("legacy catalog size = %d", len(expectedIDs))
	}
	for _, publicID := range expectedIDs {
		if _, err := service.GetLegacyV2ByPublicID(ctx, publicID); err != nil {
			t.Fatalf("resolve %s: %v", publicID, err)
		}
	}
	image, err := service.GetLegacyV2ByPublicID(ctx, "grok-imagine-image")
	if err != nil || image.PublicID != "grok-imagine-image-quality" || image.UpstreamModel != "grok-imagine-image-quality" {
		t.Fatalf("legacy quality image route = %#v, err = %v", image, err)
	}
	pro, err := service.GetLegacyV2ByPublicID(ctx, "grok-imagine-image-pro")
	if err != nil || pro.PublicID != "grok-imagine-image-quality" {
		t.Fatalf("legacy pro image route = %#v, err = %v", pro, err)
	}

	native, err := service.ListEnabled(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(native) != 8 {
		t.Fatalf("native list contains %d models, want 8 v3 canonical models", len(native))
	}
	listed, err := service.ListLegacyV2Enabled(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listed) != len(expectedIDs) {
		t.Fatalf("listed %d models, want exactly %d", len(listed), len(expectedIDs))
	}
	for index, publicID := range expectedIDs {
		if listed[index].PublicID != publicID {
			t.Fatalf("model[%d] = %q, want %q", index, listed[index].PublicID, publicID)
		}
	}
}

func TestModelProviderFilterAcceptsOnlyKnownProviders(t *testing.T) {
	for _, value := range []string{"", string(account.ProviderBuild), string(account.ProviderWeb)} {
		if !validModelFilter(value, "", string(account.ProviderBuild), string(account.ProviderWeb)) {
			t.Fatalf("known provider rejected: %q", value)
		}
	}
	if validModelFilter("cli", "", string(account.ProviderBuild), string(account.ProviderWeb)) {
		t.Fatal("unsupported provider filter was accepted")
	}
}

func TestSyncAggregatesCapabilitiesFromAllAccounts(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "model-sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := cipher.Encrypt("access-token")
	if err != nil {
		t.Fatal(err)
	}

	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	first, _, err := accountRepo.UpsertByIdentity(ctx, account.Credential{Provider: account.ProviderBuild, Name: "basic", SourceKey: "basic", EncryptedAccessToken: encrypted, ExpiresAt: time.Now().Add(time.Hour), AuthStatus: account.AuthStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := accountRepo.UpsertByIdentity(ctx, account.Credential{Provider: account.ProviderBuild, Name: "premium", SourceKey: "premium", EncryptedAccessToken: encrypted, ExpiresAt: time.Now().Add(time.Hour), AuthStatus: account.AuthStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	webAccount, _, err := accountRepo.UpsertByIdentity(ctx, account.Credential{Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, WebTier: account.WebTierSuper, Name: "web-super", SourceKey: "web-super", EncryptedAccessToken: encrypted, ExpiresAt: time.Now().Add(time.Hour), AuthStatus: account.AuthStatusActive})
	if err != nil {
		t.Fatal(err)
	}
	adapter := &modelCapabilityAdapter{models: map[uint64][]string{
		first.ID:  {"grok-basic"},
		second.ID: {"grok-basic", "grok-premium"},
	}}
	webAdapter := &modelCapabilityAdapter{provider: account.ProviderWeb, models: map[uint64][]string{
		webAccount.ID: {"grok-chat-fast", "grok-chat-auto"},
	}}
	registry := provider.NewRegistry(adapter, webAdapter)
	sticky := memory.NewStickyStore()
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), sticky, registry, cipher, nil)
	service := NewService(modelRepo, accountRepo, accountService, registry)

	count, err := service.Sync(ctx)
	if err != nil || count != 4 {
		t.Fatalf("sync count = %d, err = %v", count, err)
	}
	if attempts := adapter.attemptCount(); attempts != 2 {
		t.Fatalf("attempts = %d", attempts)
	}
	if attempts := webAdapter.attemptCount(); attempts != 1 {
		t.Fatalf("web attempts = %d", attempts)
	}
	candidates, err := accountRepo.ListRoutingCandidates(ctx, account.ProviderBuild, "grok-premium", "")
	if err != nil {
		t.Fatal(err)
	}
	support := make(map[uint64]bool, len(candidates))
	for _, candidate := range candidates {
		if !candidate.ModelCapabilityKnown {
			t.Fatalf("capability unknown for account %d", candidate.Credential.ID)
		}
		support[candidate.Credential.ID] = candidate.SupportsModel
	}
	if support[first.ID] || !support[second.ID] {
		t.Fatalf("support = %#v", support)
	}
	webCandidates, err := accountRepo.ListRoutingCandidates(ctx, account.ProviderWeb, "grok-chat-auto", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(webCandidates) != 1 || !webCandidates[0].ModelCapabilityKnown || !webCandidates[0].SupportsModel {
		t.Fatalf("web candidates = %#v", webCandidates)
	}
}

func TestSyncAccountRunsUpstreamDiscoveryConcurrently(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "model-account-sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := cipher.Encrypt("access-token")
	if err != nil {
		t.Fatal(err)
	}

	const accountCount = 10
	accountRepo := relational.NewAccountRepository(database)
	modelRepo := relational.NewModelRepository(database)
	auditRepo := relational.NewAuditRepository(database)
	adapter := &modelCapabilityAdapter{
		models:  make(map[uint64][]string, accountCount),
		entered: make(chan struct{}, accountCount),
		release: make(chan struct{}),
	}
	accountIDs := make([]uint64, 0, accountCount)
	for index := range accountCount {
		value, _, createErr := accountRepo.UpsertByIdentity(ctx, account.Credential{
			Provider: account.ProviderBuild, Name: fmt.Sprintf("account-%d", index), SourceKey: fmt.Sprintf("account-%d", index),
			EncryptedAccessToken: encrypted, ExpiresAt: time.Now().Add(time.Hour), AuthStatus: account.AuthStatusActive,
		})
		if createErr != nil {
			t.Fatal(createErr)
		}
		accountIDs = append(accountIDs, value.ID)
		adapter.models[value.ID] = []string{"grok-shared"}
	}
	registry := provider.NewRegistry(adapter)
	accountService := accountapp.NewService(accountRepo, auditRepo, memory.NewDeviceSessionStore(), memory.NewStickyStore(), registry, cipher, nil)
	service := NewService(modelRepo, accountRepo, accountService, registry)

	results := make(chan error, accountCount)
	for _, accountID := range accountIDs {
		go func() {
			_, syncErr := service.SyncAccount(ctx, accountID)
			results <- syncErr
		}()
	}
	deadline := time.NewTimer(time.Second)
	for range accountCount {
		select {
		case <-adapter.entered:
		case <-deadline.C:
			close(adapter.release)
			t.Fatalf("upstream discovery peak = %d, want %d", adapter.peak.Load(), accountCount)
		}
	}
	deadline.Stop()
	close(adapter.release)
	for range accountCount {
		if syncErr := <-results; syncErr != nil {
			t.Fatal(syncErr)
		}
	}
	if adapter.peak.Load() != accountCount {
		t.Fatalf("upstream discovery peak = %d, want %d", adapter.peak.Load(), accountCount)
	}
}

type modelCapabilityAdapter struct {
	provider account.Provider
	mu       sync.Mutex
	models   map[uint64][]string
	attempts []uint64
	entered  chan struct{}
	release  chan struct{}
	active   atomic.Int64
	peak     atomic.Int64
}

func (a *modelCapabilityAdapter) Provider() account.Provider {
	if a.provider == "" {
		return account.ProviderBuild
	}
	return a.provider
}
func (a *modelCapabilityAdapter) ListModels(ctx context.Context, credential account.Credential) ([]string, error) {
	a.mu.Lock()
	a.attempts = append(a.attempts, credential.ID)
	models := append([]string(nil), a.models[credential.ID]...)
	a.mu.Unlock()
	if a.entered == nil {
		return models, nil
	}
	current := a.active.Add(1)
	defer a.active.Add(-1)
	for {
		peak := a.peak.Load()
		if current <= peak || a.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	a.entered <- struct{}{}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-a.release:
		return models, nil
	}
}
func (a *modelCapabilityAdapter) attemptCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.attempts)
}
func (a *modelCapabilityAdapter) ForwardResponse(context.Context, provider.ResponseResourceRequest) (*provider.Response, error) {
	return &provider.Response{StatusCode: http.StatusOK}, nil
}
func (a *modelCapabilityAdapter) GetBilling(context.Context, account.Credential) (account.Billing, error) {
	return account.Billing{}, nil
}
func (a *modelCapabilityAdapter) RefreshCredential(context.Context, account.Credential) (provider.RefreshedCredential, error) {
	return provider.RefreshedCredential{}, nil
}
func (a *modelCapabilityAdapter) StartDeviceAuthorization(context.Context) (provider.DeviceAuthorization, error) {
	return provider.DeviceAuthorization{}, nil
}
func (a *modelCapabilityAdapter) PollDeviceAuthorization(context.Context, string) (provider.CredentialSeed, error) {
	return provider.CredentialSeed{}, nil
}
func (a *modelCapabilityAdapter) ParseImportedCredentials([]byte) ([]provider.CredentialSeed, error) {
	return nil, nil
}
func (a *modelCapabilityAdapter) MarshalCredentials([]provider.CredentialSeed) ([]byte, error) {
	return nil, nil
}
