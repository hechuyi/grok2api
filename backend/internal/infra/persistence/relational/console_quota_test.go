package relational

import (
	"context"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
)

func TestConsoleQuotaFirstUseCreatesAndDecrementsIndependentWindow(t *testing.T) {
	ctx := context.Background()
	repository := NewAccountRepository(openTestDatabase(t))
	credential, _, err := repository.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, Name: "console-first-use",
		SourceKey: "console-first-use", EncryptedAccessToken: testEncryptedToken, AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	updated, err := repository.DecrementQuotaWindowBy(ctx, credential.ID, account.ConsoleQuotaMode, 1, now)
	if err != nil || !updated {
		t.Fatalf("updated=%v err=%v", updated, err)
	}
	windows, err := repository.GetQuotaWindows(ctx, []uint64{credential.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(windows[credential.ID]) != 1 {
		t.Fatalf("windows = %#v", windows[credential.ID])
	}
	window := windows[credential.ID][0]
	if window.Mode != account.ConsoleQuotaMode || window.Remaining != account.ConsoleQuotaLimit-1 || window.Total != account.ConsoleQuotaLimit || window.WindowSeconds != int(account.ConsoleQuotaWindow/time.Second) {
		t.Fatalf("window = %#v", window)
	}
	if window.ResetAt == nil || !window.ResetAt.Equal(now.Add(account.ConsoleQuotaWindow)) {
		t.Fatalf("resetAt = %v", window.ResetAt)
	}
}

func TestConsoleQuotaFirst429UpsertsExhaustedWindow(t *testing.T) {
	ctx := context.Background()
	repository := NewAccountRepository(openTestDatabase(t))
	credential, _, err := repository.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, Name: "console-first-429",
		SourceKey: "console-first-429", EncryptedAccessToken: testEncryptedToken, AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	if err := repository.ExhaustQuotaWindow(ctx, credential.ID, account.ConsoleQuotaMode, nil, now); err != nil {
		t.Fatal(err)
	}
	windows, err := repository.GetQuotaWindows(ctx, []uint64{credential.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(windows[credential.ID]) != 1 {
		t.Fatalf("windows = %#v", windows[credential.ID])
	}
	window := windows[credential.ID][0]
	if window.Remaining != 0 || window.Total != account.ConsoleQuotaLimit || window.ResetAt == nil || !window.ResetAt.Equal(now.Add(account.ConsoleQuotaRateLimitCooldown)) {
		t.Fatalf("window = %#v", window)
	}
}

func TestConsoleRoutingIgnoresWeeklyWebQuota(t *testing.T) {
	ctx := context.Background()
	repository := NewAccountRepository(openTestDatabase(t))
	credential, _, err := repository.UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, AuthType: account.AuthTypeSSO, Name: "console-independent",
		SourceKey: "console-independent", EncryptedAccessToken: testEncryptedToken, AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	resetAt := now.Add(time.Hour)
	if err := repository.SaveQuotaWindows(ctx, credential.ID, account.WebTierSuper, now, []account.QuotaWindow{
		{AccountID: credential.ID, Mode: "weekly", Remaining: 100, Total: 100, ResetAt: &resetAt, SyncedAt: &now, Source: account.QuotaSourceUpstream, UpdatedAt: now},
		{AccountID: credential.ID, Mode: account.ConsoleQuotaMode, Remaining: 0, Total: account.ConsoleQuotaLimit, ResetAt: &resetAt, SyncedAt: &now, Source: account.QuotaSourceEstimated, UpdatedAt: now},
	}); err != nil {
		t.Fatal(err)
	}
	candidates, err := repository.ListRoutingCandidates(ctx, account.ProviderWeb, "", account.ConsoleQuotaMode)
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 1 || candidates[0].QuotaWindow == nil || candidates[0].QuotaWindow.Mode != account.ConsoleQuotaMode || candidates[0].QuotaWindow.Remaining != 0 {
		t.Fatalf("candidate = %#v", candidates)
	}
}
