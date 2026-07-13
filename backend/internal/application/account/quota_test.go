package account

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	accountdomain "github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
)

func TestNewQuotaViewFreeUsesObservedRollingTokens(t *testing.T) {
	quota := newQuotaView(&accountdomain.Billing{IsUnifiedBillingUser: true}, 250_000, nil, "grok-4.5-build-free")
	if quota.Type != QuotaTypeFree || quota.Unit != "tokens" || quota.Limit != 1_000_000 || quota.LimitKnown || quota.Confidence != "observed" {
		t.Fatalf("quota = %#v", quota)
	}
	if quota.Used != 250_000 || quota.Remaining != 750_000 || quota.UsagePercent != 25 || quota.WindowHours != 24 || !quota.Observed {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestNewQuotaViewPaidUsesMonthlyBilling(t *testing.T) {
	quota := newQuotaView(&accountdomain.Billing{MonthlyLimit: 200, Used: 50, BillingPeriodStart: "start", BillingPeriodEnd: "end"}, 900_000, nil, "")
	if quota.Type != QuotaTypePaid || quota.Unit != "credits" || quota.Limit != 200 {
		t.Fatalf("quota = %#v", quota)
	}
	if quota.Used != 50 || quota.Remaining != 150 || quota.UsagePercent != 25 || quota.Observed || !quota.LimitKnown {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestNewQuotaViewPaidShowsBillingProbeState(t *testing.T) {
	now := time.Now().UTC()
	next := now.Add(time.Hour)
	quota := newQuotaView(&accountdomain.Billing{MonthlyLimit: 100, Used: 100}, 0, &accountdomain.QuotaRecovery{
		Kind: accountdomain.QuotaRecoveryKindPaid, Status: accountdomain.QuotaRecoveryStatusExhausted,
		ExhaustedAt: &now, NextProbeAt: &next,
	}, "")
	if quota.Type != QuotaTypePaid || quota.Status != QuotaStatusWaitingReset || quota.NextProbeAt == nil {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestNewQuotaViewUnknownWithoutBillingSnapshot(t *testing.T) {
	quota := newQuotaView(nil, 100, nil, "")
	if quota.Type != QuotaTypeUnknown {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestNewQuotaViewEstimatesFreeFromObservedZeroBillingProfile(t *testing.T) {
	quota := newQuotaView(&accountdomain.Billing{IsUnifiedBillingUser: true, TopUpMethod: "TOP_UP_METHOD_SAVED_PAYMENT_METHOD"}, 100, nil, "")
	if quota.Type != QuotaTypeFree || quota.Source != "billingProfile" || quota.Confidence != "estimated" || quota.Limit != 1_000_000 || quota.LimitKnown {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestNewQuotaViewUsesConfirmedExhaustion(t *testing.T) {
	now := time.Now().UTC()
	next := now.Add(24 * time.Hour)
	quota := newQuotaView(&accountdomain.Billing{}, 250_000, &accountdomain.QuotaRecovery{
		Status: accountdomain.QuotaRecoveryStatusExhausted, ConfirmedUsed: 1_065_387,
		ConfirmedLimit: 1_000_000, ExhaustedAt: &now, NextProbeAt: &next, LastConfirmedAt: &now,
	}, "")
	if quota.Type != QuotaTypeFree || quota.Status != QuotaStatusWaitingReset || !quota.Confirmed {
		t.Fatalf("quota = %#v", quota)
	}
	if quota.Used != 1_065_387 || quota.Limit != 1_000_000 || quota.Remaining != 0 || quota.NextProbeAt == nil || !quota.LimitKnown {
		t.Fatalf("quota = %#v", quota)
	}
}

func TestConsole429UsesIndependentFourHourCooldown(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "console-429.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	repository := relational.NewAccountRepository(database)
	credential, _, err := repository.UpsertByIdentity(ctx, accountdomain.Credential{
		Provider: accountdomain.ProviderWeb, AuthType: accountdomain.AuthTypeSSO, Name: "console-429",
		SourceKey: "console-429", EncryptedAccessToken: "encrypted", AuthStatus: accountdomain.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	shortReset := now.Add(30 * time.Minute)
	if err := repository.SaveQuotaWindows(ctx, credential.ID, accountdomain.WebTierBasic, now, []accountdomain.QuotaWindow{{
		AccountID: credential.ID, Mode: accountdomain.ConsoleQuotaMode, Remaining: 12, Total: accountdomain.ConsoleQuotaLimit,
		WindowSeconds: int(accountdomain.ConsoleQuotaWindow / time.Second), ResetAt: &shortReset, SyncedAt: &now,
		Source: accountdomain.QuotaSourceEstimated, UpdatedAt: now,
	}}); err != nil {
		t.Fatal(err)
	}
	service := NewService(repository, nil, nil, nil, nil, nil, nil)
	service.now = func() time.Time { return now }
	exhausted, err := service.ReconcileWebRateLimit(ctx, credential.ID, accountdomain.ConsoleQuotaMode, 0)
	if err != nil || !exhausted {
		t.Fatalf("exhausted=%v err=%v", exhausted, err)
	}
	windows, err := repository.GetQuotaWindows(ctx, []uint64{credential.ID})
	if err != nil {
		t.Fatal(err)
	}
	window := windows[credential.ID][0]
	if window.Remaining != 0 || window.ResetAt == nil || !window.ResetAt.Equal(now.Add(accountdomain.ConsoleQuotaRateLimitCooldown)) {
		t.Fatalf("window = %#v", window)
	}
}
