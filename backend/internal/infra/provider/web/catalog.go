package web

import (
	"github.com/chenyme/grok2api/backend/internal/domain/account"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
)

type ModelSpec struct {
	PublicID      string
	UpstreamModel string
	ProtocolModel string
	Capability    modeldomain.Capability
	Mode          string
	MinimumTier   account.WebTier
	PreferBest    bool
}

var catalog = []ModelSpec{
	{PublicID: "grok-chat-fast", UpstreamModel: "grok-chat-fast", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierBasic},
	{PublicID: "grok-chat-auto", UpstreamModel: "grok-chat-auto", Capability: modeldomain.CapabilityChat, Mode: "auto", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-chat-expert", UpstreamModel: "grok-chat-expert", Capability: modeldomain.CapabilityChat, Mode: "expert", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-chat-heavy", UpstreamModel: "grok-chat-heavy", Capability: modeldomain.CapabilityChat, Mode: "heavy", MinimumTier: account.WebTierHeavy},
	{PublicID: "grok-imagine-image", UpstreamModel: "grok-imagine-image", ProtocolModel: "imagine-lite", Capability: modeldomain.CapabilityImage, Mode: "fast", MinimumTier: account.WebTierBasic},
	{PublicID: "grok-imagine-image-quality", UpstreamModel: "grok-imagine-image-quality", ProtocolModel: "imagine", Capability: modeldomain.CapabilityImage, MinimumTier: account.WebTierSuper},
	{PublicID: "grok-imagine-image-edit", UpstreamModel: "imagine-image-edit", Capability: modeldomain.CapabilityImageEdit, MinimumTier: account.WebTierSuper},
	{PublicID: "grok-imagine-video", UpstreamModel: "grok-imagine-video", ProtocolModel: "imagine-video-gen", Capability: modeldomain.CapabilityVideo, MinimumTier: account.WebTierSuper},
}

// legacyV2Catalog retains the old public routing profiles as real internal
// routes. This is not just name translation: mode, minimum account tier and
// the historical heavy-to-basic preference affect quota correctness.
var legacyV2Catalog = []ModelSpec{
	{PublicID: "grok-4.20-0309-non-reasoning", UpstreamModel: "grok-4.20-0309-non-reasoning", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierBasic},
	{PublicID: "grok-4.20-0309", UpstreamModel: "grok-4.20-0309", Capability: modeldomain.CapabilityChat, Mode: "auto", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-4.20-0309-reasoning", UpstreamModel: "grok-4.20-0309-reasoning", Capability: modeldomain.CapabilityChat, Mode: "expert", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-4.20-0309-non-reasoning-super", UpstreamModel: "grok-4.20-0309-non-reasoning-super", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-4.20-0309-super", UpstreamModel: "grok-4.20-0309-super", Capability: modeldomain.CapabilityChat, Mode: "auto", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-4.20-0309-reasoning-super", UpstreamModel: "grok-4.20-0309-reasoning-super", Capability: modeldomain.CapabilityChat, Mode: "expert", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-4.20-0309-non-reasoning-heavy", UpstreamModel: "grok-4.20-0309-non-reasoning-heavy", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierHeavy},
	{PublicID: "grok-4.20-0309-heavy", UpstreamModel: "grok-4.20-0309-heavy", Capability: modeldomain.CapabilityChat, Mode: "auto", MinimumTier: account.WebTierHeavy},
	{PublicID: "grok-4.20-0309-reasoning-heavy", UpstreamModel: "grok-4.20-0309-reasoning-heavy", Capability: modeldomain.CapabilityChat, Mode: "expert", MinimumTier: account.WebTierHeavy},
	{PublicID: "grok-4.20-multi-agent-0309", UpstreamModel: "grok-4.20-multi-agent-0309", Capability: modeldomain.CapabilityChat, Mode: "heavy", MinimumTier: account.WebTierHeavy},
	{PublicID: "grok-4.20-fast", UpstreamModel: "grok-4.20-fast", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierBasic, PreferBest: true},
	{PublicID: "grok-4.3-fast", UpstreamModel: "grok-4.3-fast", Capability: modeldomain.CapabilityChat, Mode: "fast", MinimumTier: account.WebTierBasic, PreferBest: true},
	{PublicID: "grok-4.20-auto", UpstreamModel: "grok-4.20-auto", Capability: modeldomain.CapabilityChat, Mode: "auto", MinimumTier: account.WebTierSuper, PreferBest: true},
	{PublicID: "grok-4.20-expert", UpstreamModel: "grok-4.20-expert", Capability: modeldomain.CapabilityChat, Mode: "expert", MinimumTier: account.WebTierSuper, PreferBest: true},
	{PublicID: "grok-4.20-heavy", UpstreamModel: "grok-4.20-heavy", Capability: modeldomain.CapabilityChat, Mode: "heavy", MinimumTier: account.WebTierHeavy, PreferBest: true},
	{PublicID: "grok-4.3-beta", UpstreamModel: "grok-4.3-beta", Capability: modeldomain.CapabilityChat, Mode: "grok-420-computer-use-sa", MinimumTier: account.WebTierSuper},
	{PublicID: "grok-imagine-image-lite", UpstreamModel: "grok-imagine-image-lite", ProtocolModel: "imagine-lite", Capability: modeldomain.CapabilityImage, Mode: "fast", MinimumTier: account.WebTierBasic},
	{PublicID: "grok-imagine-image-pro", UpstreamModel: "grok-imagine-image-pro", ProtocolModel: "imagine", Capability: modeldomain.CapabilityImage, MinimumTier: account.WebTierSuper},
}

var fullCatalog = func() []ModelSpec {
	values := make([]ModelSpec, 0, len(catalog)+len(legacyV2Catalog)+len(consoleCatalog))
	values = append(values, catalog...)
	values = append(values, legacyV2Catalog...)
	for _, spec := range consoleCatalog {
		values = append(values, ModelSpec{
			PublicID: spec.PublicID, UpstreamModel: spec.PublicID, ProtocolModel: spec.UpstreamModel,
			Capability: modeldomain.CapabilityChat, Mode: consoleQuotaMode, MinimumTier: account.WebTierBasic,
		})
	}
	return values
}()

func Catalog() []ModelSpec {
	return append([]ModelSpec(nil), fullCatalog...)
}

func Routes() []modeldomain.Route {
	models := Catalog()
	values := make([]modeldomain.Route, 0, len(models))
	for _, spec := range models {
		values = append(values, modeldomain.Route{PublicID: spec.PublicID, Provider: account.ProviderWeb, UpstreamModel: spec.UpstreamModel, Capability: spec.Capability, Enabled: true})
	}
	return values
}

func Resolve(upstreamModel string) (ModelSpec, bool) {
	for _, spec := range fullCatalog {
		if spec.UpstreamModel == upstreamModel {
			return spec, true
		}
	}
	return ModelSpec{}, false
}

func TierSupports(actual, minimum account.WebTier) bool {
	rank := map[account.WebTier]int{account.WebTierBasic: 1, account.WebTierSuper: 2, account.WebTierHeavy: 3}
	return rank[actual] >= rank[minimum]
}
