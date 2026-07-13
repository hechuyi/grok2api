package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	"github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

func TestConsoleCatalogPreservesNativeModelsAndIndependentQuota(t *testing.T) {
	tests := []struct {
		publicID string
		model    string
		effort   string
	}{
		{publicID: "grok-4.3-console", model: "grok-4.3"},
		{publicID: "grok-4.3-low", model: "grok-4.3", effort: "low"},
		{publicID: "grok-4.3-medium", model: "grok-4.3", effort: "medium"},
		{publicID: "grok-4.3-high", model: "grok-4.3", effort: "high"},
		{publicID: "grok-4.20-0309-reasoning-console", model: "grok-4.20-0309-reasoning"},
		{publicID: "grok-4.20-0309-console", model: "grok-4.20-0309"},
		{publicID: "grok-4.20-0309-non-reasoning-console", model: "grok-4.20-0309-non-reasoning"},
		{publicID: "grok-4.20-multi-agent-console", model: "grok-4.20-multi-agent-0309"},
		{publicID: "grok-4.20-multi-agent-low", model: "grok-4.20-multi-agent-0309", effort: "low"},
		{publicID: "grok-4.20-multi-agent-medium", model: "grok-4.20-multi-agent-0309", effort: "medium"},
		{publicID: "grok-4.20-multi-agent-high", model: "grok-4.20-multi-agent-0309", effort: "high"},
		{publicID: "grok-4.20-multi-agent-xhigh", model: "grok-4.20-multi-agent-0309", effort: "xhigh"},
		{publicID: "grok-build-console", model: "grok-build-0.1"},
	}
	for _, test := range tests {
		spec, ok := resolveConsole(test.publicID)
		if !ok {
			t.Fatalf("missing console model %q", test.publicID)
		}
		if spec.UpstreamModel != test.model || spec.FixedEffort != test.effort || spec.Mode != consoleQuotaMode {
			t.Fatalf("console model %q = %#v", test.publicID, spec)
		}
	}
}

func TestConsoleModelsAreRegisteredAsNativeWebRoutes(t *testing.T) {
	routes := Routes()
	found := make(map[string]bool, len(routes))
	for _, route := range routes {
		found[route.PublicID] = route.UpstreamModel == route.PublicID
	}
	for _, publicID := range []string{"grok-4.3-console", "grok-4.20-multi-agent-xhigh", "grok-build-console"} {
		if !found[publicID] {
			t.Fatalf("missing native console route %q", publicID)
		}
	}
	adapter := &Adapter{}
	if adapter.QuotaMode("grok-4.20-multi-agent-xhigh") != consoleQuotaMode {
		t.Fatalf("console quota mode = %q", adapter.QuotaMode("grok-4.20-multi-agent-xhigh"))
	}
	if adapter.PricingModel("grok-build-console") != "grok-build-0.1" {
		t.Fatalf("console pricing model = %q", adapter.PricingModel("grok-build-console"))
	}
}

func TestForwardResponseDispatchesConsoleModelsBeforeWebProtocol(t *testing.T) {
	response, err := (&Adapter{}).ForwardResponse(context.Background(), provider.ResponseResourceRequest{
		Method: http.MethodGet, Path: "/responses/resp_1", Model: "grok-4.3-console",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d", response.StatusCode)
	}
}

func TestNormalizeConsolePayloadAppliesModelEffortToolsAndLimits(t *testing.T) {
	body := []byte(`{
		"model":"grok-4.20-multi-agent-xhigh",
		"input":[{"role":"user","content":"hello"}],
		"reasoning":{"effort":"low"},
		"tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}],
		"stream":false
	}`)
	spec, _ := resolveConsole("grok-4.20-multi-agent-xhigh")
	normalized, err := normalizeConsolePayload(body, spec)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(normalized, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "grok-4.20-multi-agent-0309" || payload["stream"] != true || payload["store"] != false {
		t.Fatalf("core console payload = %#v", payload)
	}
	if payload["max_output_tokens"] != float64(2_000_000) {
		t.Fatalf("max_output_tokens = %#v", payload["max_output_tokens"])
	}
	reasoning, _ := payload["reasoning"].(map[string]any)
	if reasoning["effort"] != "xhigh" {
		t.Fatalf("reasoning = %#v", reasoning)
	}
	tools, _ := payload["tools"].([]any)
	if len(tools) != 3 || tools[0].(map[string]any)["type"] != "web_search" || tools[1].(map[string]any)["type"] != "x_search" || tools[2].(map[string]any)["name"] != "lookup" {
		t.Fatalf("tools = %#v", tools)
	}
	include, _ := payload["include"].([]any)
	if len(include) != 1 || include[0] != "reasoning.encrypted_content" {
		t.Fatalf("include = %#v", include)
	}
}

func TestNormalizeConsolePayloadUsesCallerEffortOnlyWhereSupported(t *testing.T) {
	tests := []struct {
		model         string
		wantReasoning bool
		wantEffort    string
	}{
		{model: "grok-4.3-console", wantReasoning: true, wantEffort: "high"},
		{model: "grok-4.20-0309-console", wantReasoning: false},
		{model: "grok-build-console", wantReasoning: false},
	}
	for _, test := range tests {
		spec, _ := resolveConsole(test.model)
		body, err := normalizeConsolePayload([]byte(`{"input":"hello","reasoning":{"effort":"high"}}`), spec)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		_ = json.Unmarshal(body, &payload)
		reasoning, exists := payload["reasoning"].(map[string]any)
		if exists != test.wantReasoning || (exists && reasoning["effort"] != test.wantEffort) {
			t.Fatalf("%s reasoning = %#v exists=%v", test.model, reasoning, exists)
		}
	}
}

func TestConsoleHeadersUseAnonymousBearerAndSSOCookies(t *testing.T) {
	headers := consoleHeaders("test-sso", &egress.Lease{UserAgent: "test-agent", CFCookies: "cf_clearance=test-clearance"})
	if headers.Get("Authorization") != "Bearer anonymous" || headers.Get("Origin") != consoleBaseURL || headers.Get("Referer") != consoleBaseURL+"/" {
		t.Fatalf("console headers = %#v", headers)
	}
	cookie := headers.Get("Cookie")
	for _, part := range []string{"sso=test-sso", "sso-rw=test-sso", "cf_clearance=test-clearance"} {
		if !strings.Contains(cookie, part) {
			t.Fatalf("cookie %q missing %q", cookie, part)
		}
	}
}

func TestCollectConsoleResponseReturnsCompletedResponsesObject(t *testing.T) {
	source := io.NopCloser(strings.NewReader(strings.Join([]string{
		"event: response.created\n",
		`data: {"type":"response.created","response":{"id":"resp_1","status":"in_progress"}}` + "\n\n",
		"event: response.output_text.delta\n",
		`data: {"type":"response.output_text.delta","delta":"hello"}` + "\n\n",
		"event: response.completed\n",
		`data: {"type":"response.completed","response":{"id":"resp_1","object":"response","model":"grok-4.3","status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}` + "\n\n",
		"data: [DONE]\n\n",
	}, "")))
	body, err := collectConsoleResponse(bufio.NewReader(source), "grok-4.3-console")
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["id"] != "resp_1" || payload["status"] != "completed" {
		t.Fatalf("response = %#v", payload)
	}
}

func TestConsoleStreamingBodyPreservesSSEAndReleasesExactlyOnce(t *testing.T) {
	releases := 0
	body := &releaseBody{ReadCloser: io.NopCloser(bytes.NewBufferString("data: [DONE]\n\n")), release: func() { releases++ }}
	response := &http.Response{Body: body}
	data, err := io.ReadAll(response.Body)
	if err != nil || string(data) != "data: [DONE]\n\n" {
		t.Fatalf("body=%q err=%v", data, err)
	}
	_ = response.Body.Close()
	_ = response.Body.Close()
	if releases != 1 {
		t.Fatalf("releases = %d", releases)
	}
}

func TestConsoleQuotaRecoveryIsLocalAndIndependent(t *testing.T) {
	window, err := (&Adapter{}).SyncQuotaMode(context.Background(), account.Credential{ID: 42}, account.ConsoleQuotaMode)
	if err != nil {
		t.Fatal(err)
	}
	if window.AccountID != 42 || window.Mode != account.ConsoleQuotaMode || window.Remaining != account.ConsoleQuotaLimit || window.Total != account.ConsoleQuotaLimit || window.WindowSeconds != int(account.ConsoleQuotaWindow/time.Second) || window.ResetAt != nil || window.Source != account.QuotaSourceEstimated {
		t.Fatalf("window = %#v", window)
	}
}
