package web

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"time"

	domainegress "github.com/chenyme/grok2api/backend/internal/domain/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/egress"
	"github.com/chenyme/grok2api/backend/internal/infra/provider"
)

func (a *Adapter) GenerateVideo(ctx context.Context, request provider.VideoRequest) (provider.VideoResult, error) {
	cfg := a.config()
	token, err := a.cipher.Decrypt(request.Credential.EncryptedAccessToken)
	if err != nil {
		return provider.VideoResult{}, err
	}
	lease, err := a.egress.Acquire(ctx, domainegress.ScopeWeb, fmt.Sprintf("%d", request.Credential.ID))
	if err != nil {
		return provider.VideoResult{}, err
	}
	defer lease.Release()
	parentID := ""
	references := make([]string, 0, len(request.ReferenceURLs))
	for _, rawReference := range request.ReferenceURLs {
		reference, referenceErr := a.prepareVideoReference(ctx, cfg, lease, token, rawReference)
		if referenceErr != nil {
			return provider.VideoResult{}, referenceErr
		}
		references = append(references, reference)
	}
	if len(references) > 0 {
		parentID, err = a.createMediaPost(ctx, cfg, lease, token, "MEDIA_POST_TYPE_IMAGE", references[0], "")
	} else {
		parentID, err = a.createMediaPost(ctx, cfg, lease, token, "MEDIA_POST_TYPE_VIDEO", "", request.Prompt)
	}
	if err != nil {
		return provider.VideoResult{}, err
	}
	segments := videoSegments(request.Duration, request.Legacy)
	if len(segments) == 0 {
		return provider.VideoResult{}, fmt.Errorf("duration 不受支持")
	}
	ratio := resolveAspectRatio(request.AspectRatio)
	resolution := request.Resolution
	if resolution == "" {
		resolution = "720p"
	}
	preset := normalizedVideoPreset(request.Preset)
	var result provider.VideoResult
	extendPostID := parentID
	elapsed := 0
	for index, segment := range segments {
		var payload map[string]any
		referer := cfg.BaseURL + "/imagine"
		if index == 0 {
			payload = videoCreatePayload(request.Prompt, parentID, ratio, resolution, segment, references, preset)
		} else {
			payload = videoExtensionPayload(request.Prompt, parentID, extendPostID, ratio, resolution, segment, preset, elapsed)
			referer = cfg.BaseURL + "/imagine/post/" + parentID
		}
		response, postErr := a.postJSONWithReferer(ctx, cfg, lease, token, cfg.BaseURL+"/rest/app-chat/conversations/new", payload, time.Duration(cfg.VideoTimeoutSeconds)*time.Second, referer)
		if postErr != nil {
			return provider.VideoResult{}, postErr
		}
		progress := request.Progress
		if progress != nil && len(segments) > 1 {
			segmentIndex := index
			progress = func(value int) {
				clamped := min(100, max(0, value))
				request.Progress(int((float64(segmentIndex) + float64(clamped)/100) / float64(len(segments)) * 100))
			}
		}
		segmentResult, postID, parseErr := parseVideoStream(response, progress)
		_ = response.Body.Close()
		if parseErr != nil {
			return provider.VideoResult{}, parseErr
		}
		if segmentResult.URL == "" {
			return provider.VideoResult{}, fmt.Errorf("视频生成完成但没有返回内容 URL")
		}
		result = segmentResult
		if index+1 < len(segments) {
			if strings.TrimSpace(postID) == "" {
				return provider.VideoResult{}, fmt.Errorf("视频分段生成完成但没有返回可扩展的 Post ID")
			}
			extendPostID = postID
		}
		elapsed += segment
	}
	return result, nil
}

func (a *Adapter) prepareVideoReference(ctx context.Context, cfg Config, lease *egress.Lease, token, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("视频参考图片 URL 不能为空")
	}
	image, err := a.loadChatImage(ctx, lease, value, 20<<20)
	if err != nil {
		return "", err
	}
	uploaded, err := a.uploadImage(ctx, cfg, lease, token, image, cfg.BaseURL+"/imagine")
	if err != nil {
		return "", err
	}
	if uploaded.URI == "" {
		return "", fmt.Errorf("上传视频参考图片后未返回 fileUri")
	}
	return uploaded.URI, nil
}

func parseVideoStream(response *http.Response, progress func(int)) (provider.VideoResult, string, error) {
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if response.StatusCode == http.StatusUnauthorized {
			return provider.VideoResult{}, "", provider.ErrUnauthorized
		}
		return provider.VideoResult{}, "", fmt.Errorf("视频上游返回 %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	var result provider.VideoResult
	var postID string
	handle := func(root map[string]any) (bool, error) {
		if errorValue, ok := root["error"].(map[string]any); ok {
			return false, fmt.Errorf("视频上游错误: %v", errorValue["message"])
		}
		stream := nestedMap(root, "result", "response", "streamingVideoGenerationResponse")
		if stream == nil {
			return false, nil
		}
		if value, ok := numberAsInt(stream["progress"]); ok && progress != nil {
			progress(value)
		}
		if value, _ := stream["videoPostId"].(string); value != "" {
			postID = value
		} else if value, _ := stream["videoId"].(string); value != "" {
			postID = value
		} else if value, _ := stream["assetId"].(string); value != "" {
			postID = value
		}
		moderated, _ := stream["moderated"].(bool)
		if moderated {
			return false, nil
		}
		if value, _ := stream["videoUrl"].(string); value != "" {
			result.URL = absoluteAssetURL(value)
			result.ContentType = "video/mp4"
			return true, nil
		}
		return false, nil
	}

	reader := bufio.NewReader(response.Body)
	prefix, _ := reader.Peek(64)
	trimmedPrefix := strings.TrimSpace(string(prefix))
	var err error
	if strings.HasPrefix(trimmedPrefix, "data:") || strings.HasPrefix(trimmedPrefix, "event:") {
		err = consumeVideoSSE(reader, handle)
	} else {
		err = consumeVideoJSON(reader, handle)
	}
	if err != nil {
		return provider.VideoResult{}, "", err
	}
	return result, postID, nil
}

func consumeVideoSSE(reader io.Reader, handle func(map[string]any) (bool, error)) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 64<<10), 8<<20)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "data:") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		}
		if line == "" || line == "[DONE]" || !strings.HasPrefix(line, "{") {
			continue
		}
		var root map[string]any
		if json.Unmarshal([]byte(line), &root) != nil {
			continue
		}
		complete, err := handle(root)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
	return scanner.Err()
}

func consumeVideoJSON(reader io.Reader, handle func(map[string]any) (bool, error)) error {
	decoder := json.NewDecoder(io.LimitReader(reader, 64<<20))
	for {
		var root map[string]any
		if err := decoder.Decode(&root); err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("解析视频上游流: %w", err)
		}
		complete, err := handle(root)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
	}
}

func nestedMap(value map[string]any, keys ...string) map[string]any {
	current := value
	for _, key := range keys {
		next, ok := current[key].(map[string]any)
		if !ok {
			return nil
		}
		current = next
	}
	return current
}

func videoSegments(seconds int, legacy bool) []int {
	if !legacy {
		if seconds < 1 || seconds > 15 {
			return nil
		}
		return []int{seconds}
	}
	switch seconds {
	case 6:
		return []int{6}
	case 10:
		return []int{10}
	case 12:
		return []int{6, 6}
	case 16:
		return []int{10, 6}
	case 20:
		return []int{10, 10}
	default:
		return nil
	}
}

func videoCreatePayload(prompt, parentID, ratio, resolution string, seconds int, references []string, preset ...string) map[string]any {
	config := map[string]any{"parentPostId": parentID, "aspectRatio": ratio, "videoLength": seconds, "resolutionName": resolution}
	if len(references) > 0 {
		config["isVideoEdit"] = false
		config["isReferenceToVideo"] = true
		config["imageReferences"] = references
	}
	return map[string]any{
		"temporary": true, "modelName": "imagine-video-gen", "message": videoPrompt(prompt, firstPreset(preset)), "enableSideBySide": true,
		"responseMetadata": map[string]any{"experiments": []any{}, "modelConfigOverride": map[string]any{"modelMap": map[string]any{"videoGenModelConfig": config}}},
	}
}

func videoExtensionPayload(prompt, parentID, extendPostID, ratio, resolution string, seconds int, preset string, elapsed int) map[string]any {
	config := map[string]any{
		"isVideoExtension": true, "videoExtensionStartTime": math.Round((float64(elapsed)+(1.0/24.0))*1_000_000) / 1_000_000,
		"extendPostId": extendPostID, "stitchWithExtendPostId": true,
		"originalPrompt": prompt, "originalPostId": parentID, "originalRefType": "ORIGINAL_REF_TYPE_VIDEO_EXTENSION",
		"mode": preset, "aspectRatio": ratio, "videoLength": seconds, "resolutionName": resolution,
		"parentPostId": parentID, "isVideoEdit": false,
	}
	return map[string]any{
		"temporary": true, "modelName": "imagine-video-gen", "message": videoPrompt(prompt, preset), "enableSideBySide": true,
		"responseMetadata": map[string]any{"experiments": []any{}, "modelConfigOverride": map[string]any{"modelMap": map[string]any{"videoGenModelConfig": config}}},
	}
}

func normalizedVideoPreset(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "fun", "normal", "spicy", "custom":
		return strings.ToLower(strings.TrimSpace(value))
	default:
		return "custom"
	}
}

func firstPreset(values []string) string {
	if len(values) == 0 {
		return "custom"
	}
	return normalizedVideoPreset(values[0])
}

func videoPrompt(prompt, preset string) string {
	mode := map[string]string{
		"fun": "extremely-crazy", "normal": "normal", "spicy": "extremely-spicy-or-crazy", "custom": "custom",
	}[normalizedVideoPreset(preset)]
	return strings.TrimSpace(prompt + " --mode=" + mode)
}
