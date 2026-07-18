package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/chenyme/grok2api/backend/internal/domain/account"
	inferencedomain "github.com/chenyme/grok2api/backend/internal/domain/inference"
	infraegress "github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
)

func TestNonStreamingContinuationPersistsInheritedConversation(t *testing.T) {
	ctx := context.Background()
	database, err := relational.OpenSQLite(ctx, filepath.Join(t.TempDir(), "responses.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.InitializeSchema(ctx); err != nil {
		t.Fatal(err)
	}
	credential, _, err := relational.NewAccountRepository(database).UpsertByIdentity(ctx, account.Credential{
		Provider: account.ProviderWeb, Name: "continuation", SourceKey: "continuation",
		EncryptedAccessToken: "stored", Enabled: true, AuthStatus: account.AuthStatusActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	states := relational.NewResponseRepository(database)
	now := time.Now().UTC()
	if err := states.SaveWebState(ctx, inferencedomain.WebResponseState{
		ResponseID: "resp_previous", AccountID: credential.ID, ConversationID: "conv_existing",
		UpstreamParentResponseID: "parent_previous", Status: "completed",
		ExpiresAt: now.Add(time.Hour), CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/index" {
			http.NotFound(writer, request)
			return
		}
		if request.URL.Path != "/rest/app-chat/conversations/conv_existing/responses" {
			t.Errorf("continuation path = %q", request.URL.Path)
			http.NotFound(writer, request)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("decode continuation payload: %v", err)
		} else if payload["parentResponseId"] != "parent_previous" {
			t.Errorf("continuation parent = %#v", payload["parentResponseId"])
		} else if _, legacy := payload["responseId"]; legacy {
			t.Error("continuation payload still contains legacy responseId")
		}
		writer.Header().Set("Content-Type", "text/event-stream")
		// Grok currently emits continuation frames directly under result, while
		// the initial turn wraps the same fields under result.response.
		_, _ = io.WriteString(writer, "data: {\"result\":{\"responseId\":\"parent_inflight\"}}\n")
		_, _ = io.WriteString(writer, "data: {\"result\":{\"responseId\":\"parent_inflight\",\"modelResponse\":{\"responseId\":\"parent_next\"}}}\n")
		_, _ = io.WriteString(writer, "data: {\"result\":{\"responseId\":\"parent_inflight\",\"token\":\"continued\",\"isThinking\":false,\"messageTag\":\"final\"}}\n")
		_, _ = io.WriteString(writer, "data: [DONE]\n")
	}))
	defer server.Close()

	cipher, err := security.NewCipher(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	encrypted, err := cipher.Encrypt("test-sso")
	if err != nil {
		t.Fatal(err)
	}
	adapter := NewAdapter(Config{BaseURL: server.URL}, infraegress.NewManager(egressRepositoryStub{}, cipher), cipher, states, nil)
	response, err := adapter.ForwardResponse(ctx, provider.ResponseResourceRequest{
		Credential: account.Credential{ID: credential.ID, EncryptedAccessToken: encrypted},
		Method:     http.MethodPost, Path: "/responses", Model: "grok-chat-fast", Operation: "responses",
		Body: []byte(`{"model":"grok-chat-fast","stream":false,"input":"continue","previous_response_id":"resp_previous"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		ID     string `json:"id"`
		Output []struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
	}
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	foundText := false
	for _, output := range result.Output {
		for _, content := range output.Content {
			foundText = foundText || content.Text == "continued"
		}
	}
	if !foundText {
		t.Fatalf("continuation output = %#v", result.Output)
	}
	state, err := states.GetWebState(ctx, result.ID, time.Now().UTC())
	if err != nil {
		t.Fatalf("new response state %q was not persisted: %v", result.ID, err)
	}
	if state.ConversationID != "conv_existing" || state.UpstreamParentResponseID != "parent_next" || state.ResponseID != result.ID {
		t.Fatalf("persisted continuation state = %#v", state)
	}
}
