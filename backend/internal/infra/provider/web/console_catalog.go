package web

import "github.com/chenyme/grok2api/backend/internal/domain/account"

const consoleQuotaMode = account.ConsoleQuotaMode

type consoleModelSpec struct {
	PublicID        string
	UpstreamModel   string
	Mode            string
	FixedEffort     string
	MaxOutputTokens int
	ReasoningField  bool
}

var consoleCatalog = []consoleModelSpec{
	{PublicID: "grok-4.3-console", UpstreamModel: "grok-4.3", ReasoningField: true},
	{PublicID: "grok-4.3-low", UpstreamModel: "grok-4.3", FixedEffort: "low", ReasoningField: true},
	{PublicID: "grok-4.3-medium", UpstreamModel: "grok-4.3", FixedEffort: "medium", ReasoningField: true},
	{PublicID: "grok-4.3-high", UpstreamModel: "grok-4.3", FixedEffort: "high", ReasoningField: true},
	{PublicID: "grok-4.20-0309-reasoning-console", UpstreamModel: "grok-4.20-0309-reasoning"},
	{PublicID: "grok-4.20-0309-console", UpstreamModel: "grok-4.20-0309"},
	{PublicID: "grok-4.20-0309-non-reasoning-console", UpstreamModel: "grok-4.20-0309-non-reasoning"},
	{PublicID: "grok-4.20-multi-agent-console", UpstreamModel: "grok-4.20-multi-agent-0309", MaxOutputTokens: 2_000_000, ReasoningField: true},
	{PublicID: "grok-4.20-multi-agent-low", UpstreamModel: "grok-4.20-multi-agent-0309", FixedEffort: "low", MaxOutputTokens: 2_000_000, ReasoningField: true},
	{PublicID: "grok-4.20-multi-agent-medium", UpstreamModel: "grok-4.20-multi-agent-0309", FixedEffort: "medium", MaxOutputTokens: 2_000_000, ReasoningField: true},
	{PublicID: "grok-4.20-multi-agent-high", UpstreamModel: "grok-4.20-multi-agent-0309", FixedEffort: "high", MaxOutputTokens: 2_000_000, ReasoningField: true},
	{PublicID: "grok-4.20-multi-agent-xhigh", UpstreamModel: "grok-4.20-multi-agent-0309", FixedEffort: "xhigh", MaxOutputTokens: 2_000_000, ReasoningField: true},
	{PublicID: "grok-build-console", UpstreamModel: "grok-build-0.1", MaxOutputTokens: 256_000},
}

func resolveConsole(publicID string) (consoleModelSpec, bool) {
	for _, spec := range consoleCatalog {
		if spec.PublicID == publicID {
			spec.Mode = consoleQuotaMode
			return spec, true
		}
	}
	return consoleModelSpec{}, false
}
