package web

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	domainegress "github.com/chenyme/grok2api/backend/internal/domain/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
	"github.com/chenyme/grok2api/backend/internal/infra/provider/conversation"
)

const (
	consoleBaseURL          = "https://console.x.ai"
	consoleResponsesPath    = "/v1/responses"
	maxConsoleResponseBytes = 128 << 20
)

func (a *Adapter) forwardConsole(ctx context.Context, request provider.ResponseResourceRequest, spec consoleModelSpec) (*provider.Response, error) {
	if request.Method != http.MethodPost || request.Path != "/responses" {
		return jsonProviderResponse(http.StatusBadRequest, map[string]any{"error": map[string]any{
			"type": "invalid_request_error", "code": "unsupported_operation", "message": "Console 模型仅支持 Responses 创建请求",
		}}), nil
	}
	body := request.Body
	var options conversation.ResponseOptions
	var err error
	if request.NormalizeBody {
		body, options, err = conversation.ConvertRequestWithOptions(body, request.Model, request.Operation)
		if err != nil {
			return consoleInvalidResponse(request.Operation, err), nil
		}
	}
	body, err = normalizeConsolePayload(body, spec)
	if err != nil {
		return consoleInvalidResponse(request.Operation, err), nil
	}
	token, err := a.cipher.Decrypt(request.Credential.EncryptedAccessToken)
	if err != nil {
		return nil, err
	}
	lease, err := a.egress.Acquire(ctx, domainegress.ScopeWeb, strconv.FormatUint(request.Credential.ID, 10))
	if err != nil {
		return nil, err
	}
	cfg := a.config()
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.ChatTimeoutSeconds)*time.Second)
	upstreamRequest, err := http.NewRequestWithContext(requestCtx, http.MethodPost, consoleBaseURL+consoleResponsesPath, bytes.NewReader(body))
	if err != nil {
		cancel()
		lease.Release()
		return nil, err
	}
	upstreamRequest.Header = consoleHeaders(token, lease)
	response, err := lease.Do(upstreamRequest)
	if err != nil {
		cancel()
		a.egress.Feedback(context.WithoutCancel(ctx), lease.NodeID, 0, err)
		lease.Release()
		return nil, err
	}
	response.Body = &releaseBody{ReadCloser: &cancelBody{ReadCloser: response.Body, cancel: cancel}, release: lease.Release}
	a.egress.Feedback(context.WithoutCancel(ctx), lease.NodeID, response.StatusCode, nil)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return &provider.Response{StatusCode: response.StatusCode, Status: response.Status, Header: response.Header.Clone(), Body: response.Body}, nil
	}
	if request.Streaming {
		streamBody := io.ReadCloser(response.Body)
		if request.Operation == conversation.OperationChat || request.Operation == conversation.OperationMessages {
			streamBody = conversation.ConvertResponseStreamWithOptions(streamBody, request.Operation, options)
		}
		return &provider.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: streamHeaders(), Body: streamBody}, nil
	}
	data, readErr := collectConsoleResponse(bufio.NewReaderSize(response.Body, 64<<10), request.Model)
	closeErr := response.Body.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if request.Operation == conversation.OperationChat || request.Operation == conversation.OperationMessages {
		data, err = conversation.ConvertResponseJSONWithOptions(data, request.Operation, options)
		if err != nil {
			return nil, err
		}
	}
	return &provider.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: jsonHeaders(), Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func consoleInvalidResponse(operation string, err error) *provider.Response {
	if operation == conversation.OperationMessages {
		return jsonProviderResponse(http.StatusBadRequest, map[string]any{"type": "error", "error": map[string]any{"type": "invalid_request_error", "message": err.Error()}})
	}
	return jsonProviderResponse(http.StatusBadRequest, map[string]any{"error": map[string]any{"type": "invalid_request_error", "message": err.Error()}})
}

func consoleHeaders(token string, lease *egress.Lease) http.Header {
	value := buildHeaders(token, lease, "application/json")
	applyAppHeaders(value, consoleBaseURL, consoleBaseURL+"/")
	value.Set("Authorization", "Bearer anonymous")
	value.Set("x-cluster", "https://us-east-1.api.x.ai")
	return value
}

func collectConsoleResponse(reader *bufio.Reader, publicModel string) ([]byte, error) {
	var event string
	var dataLines []string
	var completed json.RawMessage
	var responseID string
	var text strings.Builder
	var consumed int
	dispatch := func() error {
		if len(dataLines) == 0 {
			event = ""
			return nil
		}
		data := strings.Join(dataLines, "\n")
		dataLines = dataLines[:0]
		if strings.TrimSpace(data) == "[DONE]" {
			event = ""
			return nil
		}
		var root map[string]json.RawMessage
		if json.Unmarshal([]byte(data), &root) != nil {
			event = ""
			return nil
		}
		typeName := event
		if typeName == "" {
			_ = json.Unmarshal(root["type"], &typeName)
		}
		switch typeName {
		case "response.created", "response.in_progress":
			var response struct {
				ID string `json:"id"`
			}
			_ = json.Unmarshal(root["response"], &response)
			if response.ID != "" {
				responseID = response.ID
			}
		case "response.output_text.delta":
			var delta string
			_ = json.Unmarshal(root["delta"], &delta)
			text.WriteString(delta)
		case "response.completed":
			if raw := root["response"]; len(raw) > 0 && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
				completed = append(completed[:0], raw...)
			}
		case "error", "response.failed":
			return fmt.Errorf("Console 上游流返回失败事件")
		}
		event = ""
		return nil
	}
	for {
		line, err := reader.ReadString('\n')
		consumed += len(line)
		if consumed > maxConsoleResponseBytes {
			return nil, fmt.Errorf("Console 上游响应超过 128 MiB")
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case line == "":
			if dispatchErr := dispatch(); dispatchErr != nil {
				return nil, dispatchErr
			}
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			dataLines = append(dataLines, strings.TrimSpace(strings.TrimPrefix(line, "data:")))
		}
		if err != nil {
			if err != io.EOF {
				return nil, err
			}
			if dispatchErr := dispatch(); dispatchErr != nil {
				return nil, dispatchErr
			}
			break
		}
	}
	if len(completed) > 0 {
		return completed, nil
	}
	if text.Len() == 0 {
		return nil, fmt.Errorf("Console 上游流缺少完成事件")
	}
	if responseID == "" {
		responseID = newWebID("resp")
	}
	return json.Marshal(map[string]any{
		"id": responseID, "object": "response", "model": publicModel, "status": "completed",
		"output": []any{map[string]any{"id": newWebID("msg"), "type": "message", "role": "assistant", "status": "completed", "content": []any{map[string]any{"type": "output_text", "text": text.String(), "annotations": []any{}}}}},
		"usage":  map[string]any{"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
	})
}
