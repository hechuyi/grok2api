package model

import modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"

// legacyV2ModelSpec keeps the v2 public contract separate from v3 provider
// routing. CanonicalID always names an existing v3 public route.
type legacyV2ModelSpec struct {
	PublicID    string
	RoutingID   string
	CanonicalID string
	Capability  modeldomain.Capability
	DisplayName string
}

// legacyV2Models preserves the registration order returned by v2 /v1/models.
// The v2 grok-imagine-image name conflicts with v3's canonical lite model, so
// it deliberately resolves to itself; v2 quality callers can use the old
// grok-imagine-image-pro name, which maps to v3's quality route.
var legacyV2Models = []legacyV2ModelSpec{
	{PublicID: "grok-4.20-0309-non-reasoning", CanonicalID: "grok-chat-fast", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Non-Reasoning"},
	{PublicID: "grok-4.20-0309", CanonicalID: "grok-chat-auto", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309"},
	{PublicID: "grok-4.20-0309-reasoning", CanonicalID: "grok-chat-expert", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Reasoning"},
	{PublicID: "grok-4.20-0309-non-reasoning-super", CanonicalID: "grok-chat-fast", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Non-Reasoning Super"},
	{PublicID: "grok-4.20-0309-super", CanonicalID: "grok-chat-auto", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Super"},
	{PublicID: "grok-4.20-0309-reasoning-super", CanonicalID: "grok-chat-expert", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Reasoning Super"},
	{PublicID: "grok-4.20-0309-non-reasoning-heavy", CanonicalID: "grok-chat-fast", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Non-Reasoning Heavy"},
	{PublicID: "grok-4.20-0309-heavy", CanonicalID: "grok-chat-auto", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Heavy"},
	{PublicID: "grok-4.20-0309-reasoning-heavy", CanonicalID: "grok-chat-expert", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Reasoning Heavy"},
	{PublicID: "grok-4.20-multi-agent-0309", CanonicalID: "grok-chat-heavy", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent 0309"},
	{PublicID: "grok-4.20-fast", CanonicalID: "grok-chat-fast", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Fast"},
	{PublicID: "grok-4.3-fast", CanonicalID: "grok-chat-fast", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 Fast"},
	{PublicID: "grok-4.20-auto", CanonicalID: "grok-chat-auto", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Auto"},
	{PublicID: "grok-4.20-expert", CanonicalID: "grok-chat-expert", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Expert"},
	{PublicID: "grok-4.20-heavy", CanonicalID: "grok-chat-heavy", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Heavy"},
	{PublicID: "grok-4.3-beta", CanonicalID: "grok-chat-auto", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 Beta"},
	{PublicID: "grok-imagine-image-lite", CanonicalID: "grok-imagine-image", Capability: modeldomain.CapabilityImage, DisplayName: "Grok Imagine Image Lite"},
	{PublicID: "grok-imagine-image", RoutingID: "grok-imagine-image-quality", CanonicalID: "grok-imagine-image-quality", Capability: modeldomain.CapabilityImage, DisplayName: "Grok Imagine Image"},
	{PublicID: "grok-imagine-image-pro", CanonicalID: "grok-imagine-image-quality", Capability: modeldomain.CapabilityImage, DisplayName: "Grok Imagine Image Pro"},
	{PublicID: "grok-imagine-image-edit", CanonicalID: "grok-imagine-image-edit", Capability: modeldomain.CapabilityImageEdit, DisplayName: "Grok Imagine Image Edit"},
	{PublicID: "grok-imagine-video", CanonicalID: "grok-imagine-video", Capability: modeldomain.CapabilityVideo, DisplayName: "Grok Imagine Video"},
	{PublicID: "grok-4.3-console", CanonicalID: "grok-4.3-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 (Console)"},
	{PublicID: "grok-4.3-low", CanonicalID: "grok-4.3-low", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 Low Thinking"},
	{PublicID: "grok-4.3-medium", CanonicalID: "grok-4.3-medium", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 Medium Thinking"},
	{PublicID: "grok-4.3-high", CanonicalID: "grok-4.3-high", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.3 High Thinking"},
	{PublicID: "grok-4.20-0309-reasoning-console", CanonicalID: "grok-4.20-0309-reasoning-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Reasoning (Console)"},
	{PublicID: "grok-4.20-0309-console", CanonicalID: "grok-4.20-0309-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 (Console)"},
	{PublicID: "grok-4.20-multi-agent-console", CanonicalID: "grok-4.20-multi-agent-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent (Console)"},
	{PublicID: "grok-4.20-multi-agent-low", CanonicalID: "grok-4.20-multi-agent-low", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent Low"},
	{PublicID: "grok-4.20-multi-agent-medium", CanonicalID: "grok-4.20-multi-agent-medium", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent Medium"},
	{PublicID: "grok-4.20-multi-agent-high", CanonicalID: "grok-4.20-multi-agent-high", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent High"},
	{PublicID: "grok-4.20-multi-agent-xhigh", CanonicalID: "grok-4.20-multi-agent-xhigh", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 Multi-Agent XHigh"},
	{PublicID: "grok-4.20-0309-non-reasoning-console", CanonicalID: "grok-4.20-0309-non-reasoning-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok 4.20 0309 Non-Reasoning (Console)"},
	{PublicID: "grok-build-console", CanonicalID: "grok-build-console", Capability: modeldomain.CapabilityChat, DisplayName: "Grok Build 0.1 (Console)"},
}

var legacyV2ModelsByPublicID = func() map[string]legacyV2ModelSpec {
	values := make(map[string]legacyV2ModelSpec, len(legacyV2Models))
	for _, value := range legacyV2Models {
		values[value.PublicID] = value
	}
	return values
}()

// DisplayName returns the stable v2 display label when one exists.
func DisplayName(publicID string) string {
	if value, ok := legacyV2ModelsByPublicID[publicID]; ok {
		return value.DisplayName
	}
	return publicID
}

// LegacyV2RoutingID returns the internal public route used for an old v2 model
// name. Most names route to an identically named compatibility route. The one
// exception is grok-imagine-image: v2 exposed the quality model under that
// name, while v3 uses it for the lite model.
func LegacyV2RoutingID(publicID string) (string, bool) {
	value, ok := legacyV2ModelsByPublicID[publicID]
	if !ok {
		return "", false
	}
	if value.RoutingID != "" {
		return value.RoutingID, true
	}
	return value.PublicID, true
}

func IsLegacyV2OnlyModel(publicID string) bool {
	if _, ok := legacyV2ModelsByPublicID[publicID]; !ok {
		return false
	}
	switch publicID {
	case "grok-imagine-image", "grok-imagine-image-edit", "grok-imagine-video":
		return false
	default:
		return true
	}
}
