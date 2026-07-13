package web

import (
	"encoding/json"
	"fmt"
	"strings"
)

const defaultConsoleMaxOutputTokens = 1_000_000

var consoleReasoningEfforts = map[string]string{
	"none": "none", "minimal": "low", "low": "low", "medium": "medium", "high": "high", "xhigh": "xhigh",
}

var consoleInternalToolNames = map[string]bool{
	"web_search": true, "x_search": true, "code_interpreter": true, "file_search": true,
	"web_search_with_snippets": true, "browse_page": true, "open_page": true, "open_page_with_find": true,
	"search_images": true, "image_search": true, "view_image": true,
	"x_user_search": true, "x_keyword_search": true, "x_semantic_search": true, "x_thread_fetch": true, "view_x_video": true,
	"chatroom_send": true, "code_execution": true, "collections_search": true,
}

func normalizeConsolePayload(body []byte, spec consoleModelSpec) ([]byte, error) {
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, fmt.Errorf("解析 Console Responses 请求: %w", err)
	}
	if payload["input"] == nil {
		return nil, fmt.Errorf("Console Responses 请求缺少 input")
	}
	payload["model"] = spec.UpstreamModel
	payload["stream"] = true
	payload["store"] = false
	payload["include"] = []any{"reasoning.encrypted_content"}
	if _, exists := payload["max_output_tokens"]; !exists {
		limit := spec.MaxOutputTokens
		if limit <= 0 {
			limit = defaultConsoleMaxOutputTokens
		}
		payload["max_output_tokens"] = limit
	}
	if spec.ReasoningField {
		effort := spec.FixedEffort
		if effort == "" {
			effort = consolePayloadEffort(payload)
		}
		payload["reasoning"] = map[string]any{"effort": effort}
	} else {
		delete(payload, "reasoning")
	}
	payload["tools"] = mergeConsoleTools(payload["tools"])
	if tools, ok := payload["tools"].([]any); !ok || len(tools) == 0 {
		delete(payload, "tools")
		delete(payload, "tool_choice")
	} else if payload["tool_choice"] == nil {
		payload["tool_choice"] = "auto"
	}
	return json.Marshal(payload)
}

func consolePayloadEffort(payload map[string]any) string {
	effort := "medium"
	if reasoning, ok := payload["reasoning"].(map[string]any); ok {
		if raw, ok := reasoning["effort"].(string); ok {
			if normalized := consoleReasoningEfforts[strings.ToLower(strings.TrimSpace(raw))]; normalized != "" {
				effort = normalized
			}
		}
	}
	return effort
}

func mergeConsoleTools(raw any) []any {
	result := []any{
		map[string]any{"type": "web_search", "enable_image_understanding": true},
		map[string]any{"type": "x_search", "enable_video_understanding": true},
	}
	values, _ := raw.([]any)
	for _, rawTool := range values {
		tool, ok := rawTool.(map[string]any)
		if !ok {
			continue
		}
		name, _ := tool["name"].(string)
		if tool["type"] == "function" && consoleInternalToolNames[strings.TrimSpace(name)] {
			continue
		}
		result = append(result, tool)
	}
	return result
}
