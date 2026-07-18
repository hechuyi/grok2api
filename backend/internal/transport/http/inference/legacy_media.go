package inference

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/chenyme/grok2api/backend/internal/application/gateway"
	clientkeydomain "github.com/chenyme/grok2api/backend/internal/domain/clientkey"
	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/chenyme/grok2api/backend/internal/infra/security"
	"github.com/gin-gonic/gin"
)

const legacyMediaFormMemory = 8 << 20

type legacyChatMediaRequest struct {
	Model    string `json:"model"`
	Messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"messages"`
	ImageConfig *struct {
		Count          *int   `json:"n"`
		Size           string `json:"size"`
		ResponseFormat string `json:"response_format"`
	} `json:"image_config"`
	VideoConfig *struct {
		Seconds        *int   `json:"seconds"`
		Size           string `json:"size"`
		ResolutionName string `json:"resolution_name"`
		Preset         string `json:"preset"`
	} `json:"video_config"`
}

type legacyChatContentBlock struct {
	Type     string          `json:"type"`
	Text     string          `json:"text"`
	ImageURL json.RawMessage `json:"image_url"`
}

func parseLegacyMediaChat(body []byte) (legacyChatMediaRequest, string, []string, error) {
	var request legacyChatMediaRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return request, "", nil, fmt.Errorf("Chat Completions 请求无效")
	}
	prompt := ""
	images := make([]string, 0, 7)
	for _, message := range request.Messages {
		var text string
		if json.Unmarshal(message.Content, &text) == nil {
			if value := strings.TrimSpace(text); value != "" {
				prompt = value
			}
			continue
		}
		var blocks []legacyChatContentBlock
		if json.Unmarshal(message.Content, &blocks) != nil {
			continue
		}
		var textParts []string
		for _, block := range blocks {
			switch block.Type {
			case "text":
				if value := strings.TrimSpace(block.Text); value != "" {
					textParts = append(textParts, value)
				}
			case "image_url":
				if value := legacyChatImageURL(block.ImageURL); value != "" {
					images = append(images, value)
				}
			}
		}
		if len(textParts) > 0 {
			prompt = strings.Join(textParts, " ")
		}
	}
	if len(images) > 7 {
		images = images[len(images)-7:]
	}
	return request, prompt, images, nil
}

func legacyChatImageURL(raw json.RawMessage) string {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		return strings.TrimSpace(value)
	}
	var object struct {
		URL string `json:"url"`
	}
	if json.Unmarshal(raw, &object) == nil {
		return strings.TrimSpace(object.URL)
	}
	return ""
}

func (h *Handler) dispatchLegacyMediaChat(c *gin.Context, body []byte, streaming bool, route modeldomain.Route, clientKey clientkeydomain.Key, requestID, routingModel string) bool {
	if !legacyMediaChatNeedsDispatch(route) {
		return false
	}
	request, prompt, images, err := parseLegacyMediaChat(body)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return true
	}
	responseModel := strings.TrimSpace(request.Model)
	if responseModel == "" {
		responseModel = routingModel
	}
	switch route.Capability {
	case modeldomain.CapabilityImage:
		h.createLegacyImageChat(c, request, prompt, streaming, clientKey, requestID, routingModel, responseModel)
	case modeldomain.CapabilityImageEdit:
		h.createLegacyImageEditChat(c, request, prompt, images, streaming, clientKey, requestID, routingModel, responseModel)
	case modeldomain.CapabilityVideo:
		h.createLegacyVideoChat(c, request, prompt, images, streaming, clientKey, requestID, routingModel, responseModel)
	}
	return true
}

func legacyMediaChatNeedsDispatch(route modeldomain.Route) bool {
	switch route.Capability {
	case modeldomain.CapabilityImage:
		// The lite route already has a native streaming Chat implementation in
		// the v3 provider. Quality/pro routes need the v2 media wrapper.
		return route.UpstreamModel != "grok-imagine-image-lite" && route.UpstreamModel != "grok-imagine-image"
	case modeldomain.CapabilityImageEdit, modeldomain.CapabilityVideo:
		return true
	default:
		return false
	}
}

func (h *Handler) createLegacyImageChat(c *gin.Context, request legacyChatMediaRequest, prompt string, streaming bool, clientKey clientkeydomain.Key, requestID, routingModel, responseModel string) {
	if prompt == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片生成需要文本提示词")
		return
	}
	count := 1
	size := "1024x1024"
	responseFormat := "url"
	if request.ImageConfig != nil {
		if request.ImageConfig.Count != nil {
			count = *request.ImageConfig.Count
		}
		if value := strings.TrimSpace(request.ImageConfig.Size); value != "" {
			size = value
		}
		if value := strings.ToLower(strings.TrimSpace(request.ImageConfig.ResponseFormat)); value != "" {
			responseFormat = value
		}
	}
	if count < 1 || count > 2 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.n 必须在 1 到 2 之间")
		return
	}
	if size != "1024x1024" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.size 必须是 1024x1024")
		return
	}
	if responseFormat != "url" && responseFormat != "b64_json" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.response_format 必须是 url 或 b64_json")
		return
	}
	result, err := h.gateway.GenerateImage(c.Request.Context(), gateway.ImageGenerationInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: routingModel, Prompt: prompt,
		Count: count, Size: size, Resolution: "1k", ResponseFormat: responseFormat, Streaming: false,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		h.writeResult(c, result, false)
		return
	}
	content, err := readLegacyImageChatResult(result)
	if err != nil {
		writeOpenAIError(c, http.StatusBadGateway, "invalid_upstream_response", "图片生成响应无效")
		return
	}
	h.writeLegacyChatCompletion(c, responseModel, content, "", streaming)
}

func (h *Handler) createLegacyImageEditChat(c *gin.Context, request legacyChatMediaRequest, prompt string, images []string, streaming bool, clientKey clientkeydomain.Key, requestID, routingModel, responseModel string) {
	if prompt == "" || len(images) == 0 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片编辑需要文本提示词和至少一张图片")
		return
	}
	count := 1
	size := "1024x1024"
	responseFormat := "url"
	if request.ImageConfig != nil {
		if request.ImageConfig.Count != nil {
			count = *request.ImageConfig.Count
		}
		if value := strings.TrimSpace(request.ImageConfig.Size); value != "" {
			size = value
		}
		if value := strings.ToLower(strings.TrimSpace(request.ImageConfig.ResponseFormat)); value != "" {
			responseFormat = value
		}
	}
	if count < 1 || count > 2 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.n 必须在 1 到 2 之间")
		return
	}
	if size != "1024x1024" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.size 必须是 1024x1024")
		return
	}
	if responseFormat != "url" && responseFormat != "b64_json" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "image_config.response_format 必须是 url 或 b64_json")
		return
	}
	result, err := h.gateway.EditImage(c.Request.Context(), gateway.ImageEditInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: routingModel, Prompt: prompt,
		ImageURLs: images, Count: count, Resolution: "1k", ResponseFormat: responseFormat,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	if result.StatusCode < 200 || result.StatusCode >= 300 {
		h.writeResult(c, result, false)
		return
	}
	content, err := readLegacyImageChatResult(result)
	if err != nil {
		writeOpenAIError(c, http.StatusBadGateway, "invalid_upstream_response", "图片编辑响应无效")
		return
	}
	h.writeLegacyChatCompletion(c, responseModel, content, "", streaming)
}

func readLegacyImageChatResult(result *gateway.Result) (string, error) {
	errorCode := ""
	defer result.Body.Close()
	defer func() { result.Finalize(gateway.Usage{}, "", errorCode) }()
	data, err := io.ReadAll(io.LimitReader(result.Body, maxJSONResponseTransferBytes+1))
	if err != nil || len(data) > maxJSONResponseTransferBytes {
		errorCode = "invalid_upstream_response"
		return "", fmt.Errorf("读取图片响应失败")
	}
	var payload struct {
		Data []struct {
			URL     string `json:"url"`
			B64JSON string `json:"b64_json"`
		} `json:"data"`
	}
	if json.Unmarshal(data, &payload) != nil || len(payload.Data) == 0 {
		errorCode = "invalid_upstream_response"
		return "", fmt.Errorf("图片响应格式无效")
	}
	values := make([]string, 0, len(payload.Data))
	for _, item := range payload.Data {
		if value := strings.TrimSpace(item.URL); value != "" {
			values = append(values, "![image]("+value+")")
		} else if value := strings.TrimSpace(item.B64JSON); value != "" {
			values = append(values, "![image](data:image/png;base64,"+value+")")
		}
	}
	if len(values) == 0 {
		errorCode = "invalid_upstream_response"
		return "", fmt.Errorf("图片响应没有内容")
	}
	return strings.Join(values, "\n\n"), nil
}

func (h *Handler) createLegacyVideoChat(c *gin.Context, request legacyChatMediaRequest, prompt string, images []string, streaming bool, clientKey clientkeydomain.Key, requestID, routingModel, responseModel string) {
	if prompt == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "视频生成需要文本提示词")
		return
	}
	duration := 6
	size := "720x1280"
	resolution := ""
	preset := "custom"
	if request.VideoConfig != nil {
		if request.VideoConfig.Seconds != nil {
			duration = *request.VideoConfig.Seconds
		}
		if value := strings.TrimSpace(request.VideoConfig.Size); value != "" {
			size = value
		}
		resolution = strings.ToLower(strings.TrimSpace(request.VideoConfig.ResolutionName))
		if value := strings.ToLower(strings.TrimSpace(request.VideoConfig.Preset)); value != "" {
			preset = value
		}
	}
	if !legacyVideoDuration(duration) {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "video_config.seconds 必须是 6、10、12、16 或 20")
		return
	}
	aspectRatio, defaultResolution, ok := legacyVideoSize(size)
	if !ok {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "video_config.size 不受支持")
		return
	}
	if resolution == "" {
		resolution = defaultResolution
	}
	if resolution != "480p" && resolution != "720p" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "video_config.resolution_name 必须是 480p 或 720p")
		return
	}
	if !legacyVideoPreset(preset) {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "video_config.preset 不受支持")
		return
	}
	job, err := h.gateway.CreateVideo(c.Request.Context(), gateway.VideoInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: routingModel, Prompt: prompt,
		Duration: duration, AspectRatio: aspectRatio, Resolution: resolution,
		ReferenceURLs: images, Preset: preset, Legacy: true, LegacySize: size,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.waitLegacyVideoChat(c, job, clientKey, responseModel, streaming)
}

func (h *Handler) waitLegacyVideoChat(c *gin.Context, job mediadomain.Job, clientKey clientkeydomain.Key, model string, streaming bool) {
	reasoning := make([]string, 0, 20)
	lastProgress := -1
	responseID := legacyChatResponseID()
	if streaming {
		c.Header("Content-Type", "text/event-stream")
		c.Header("Cache-Control", "no-cache")
		c.Status(http.StatusOK)
		_ = writeLegacyChatChunk(c, responseID, model, gin.H{"role": "assistant"}, nil)
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		if job.Progress != lastProgress {
			lastProgress = job.Progress
			message := fmt.Sprintf("视频正在生成 %d%%", min(100, max(0, job.Progress)))
			reasoning = append(reasoning, message)
			if streaming {
				_ = writeLegacyChatChunk(c, responseID, model, gin.H{"reasoning_content": message + "\n"}, nil)
			}
		}
		switch job.Status {
		case mediadomain.StatusCompleted:
			if streaming {
				_ = writeLegacyChatChunk(c, responseID, model, gin.H{"content": job.UpstreamURL}, nil)
				finish := "stop"
				_ = writeLegacyChatChunk(c, responseID, model, gin.H{}, &finish)
				_, _ = c.Writer.Write([]byte("data: [DONE]\n\n"))
				c.Writer.Flush()
				return
			}
			h.writeLegacyChatCompletion(c, model, job.UpstreamURL, strings.Join(reasoning, "\n"), false)
			return
		case mediadomain.StatusFailed:
			if streaming {
				_ = writeLegacyChatError(c, "视频生成失败")
				return
			}
			writeOpenAIError(c, http.StatusBadGateway, "video_generation_failed", "视频生成失败")
			return
		}
		select {
		case <-c.Request.Context().Done():
			if streaming {
				_ = writeLegacyChatError(c, "视频生成请求已取消")
				return
			}
			writeOpenAIError(c, http.StatusGatewayTimeout, "video_generation_timeout", "视频生成等待超时")
			return
		case <-ticker.C:
			current, err := h.gateway.GetVideo(c.Request.Context(), job.ID, clientKey)
			if err != nil {
				if streaming {
					_ = writeLegacyChatError(c, "读取视频任务失败")
					return
				}
				writeGatewayError(c, err)
				return
			}
			job = current
		}
	}
}

func legacyChatResponseID() string {
	value, err := security.NewOpaqueToken(18)
	if err != nil || value == "" {
		value = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return "chatcmpl_" + value
}

func legacyChatCompletion(model, content, reasoning string) gin.H {
	message := gin.H{"role": "assistant", "content": content}
	if reasoning != "" {
		message["reasoning_content"] = reasoning
	}
	return gin.H{
		"id": legacyChatResponseID(), "object": "chat.completion", "created": time.Now().Unix(), "model": model,
		"choices": []gin.H{{"index": 0, "message": message, "finish_reason": "stop"}},
		"usage":   gin.H{"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0},
	}
}

func (h *Handler) writeLegacyChatCompletion(c *gin.Context, model, content, reasoning string, streaming bool) {
	if !streaming {
		c.JSON(http.StatusOK, legacyChatCompletion(model, content, reasoning))
		return
	}
	responseID := legacyChatResponseID()
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Status(http.StatusOK)
	_ = writeLegacyChatChunk(c, responseID, model, gin.H{"role": "assistant"}, nil)
	if reasoning != "" {
		_ = writeLegacyChatChunk(c, responseID, model, gin.H{"reasoning_content": reasoning}, nil)
	}
	_ = writeLegacyChatChunk(c, responseID, model, gin.H{"content": content}, nil)
	finish := "stop"
	_ = writeLegacyChatChunk(c, responseID, model, gin.H{}, &finish)
	_, _ = c.Writer.Write([]byte("data: [DONE]\n\n"))
	c.Writer.Flush()
}

func writeLegacyChatChunk(c *gin.Context, responseID, model string, delta gin.H, finishReason *string) error {
	payload := gin.H{
		"id": responseID, "object": "chat.completion.chunk", "created": time.Now().Unix(), "model": model,
		"choices": []gin.H{{"index": 0, "delta": delta, "finish_reason": finishReason}},
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	if err := setResponseWriteDeadline(c.Writer); err != nil {
		return err
	}
	_, err = c.Writer.Write([]byte("data: " + string(data) + "\n\n"))
	if err == nil {
		c.Writer.Flush()
	}
	return err
}

func writeLegacyChatError(c *gin.Context, message string) error {
	data, _ := json.Marshal(gin.H{"error": gin.H{"message": message, "type": "server_error"}})
	_, err := c.Writer.Write([]byte("event: error\ndata: " + string(data) + "\n\ndata: [DONE]\n\n"))
	c.Writer.Flush()
	return err
}

func (h *Handler) editLegacyImage(c *gin.Context) {
	if err := h.parseLegacyMultipart(c); err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	model := strings.TrimSpace(c.PostForm("model"))
	prompt := strings.TrimSpace(c.PostForm("prompt"))
	if model == "" || prompt == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "图片编辑缺少有效 model 或 prompt")
		return
	}
	count, err := legacyFormInteger(c.PostForm("n"), 1)
	if err != nil || count < 1 || count > 2 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "n 必须在 1 到 2 之间")
		return
	}
	size := strings.ToLower(strings.TrimSpace(c.PostForm("size")))
	if size == "" {
		size = "1024x1024"
	}
	if size != "1024x1024" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "size 必须是 1024x1024")
		return
	}
	responseFormat := strings.ToLower(strings.TrimSpace(c.PostForm("response_format")))
	if responseFormat == "" {
		responseFormat = "url"
	}
	if responseFormat != "url" && responseFormat != "b64_json" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "response_format 必须是 url 或 b64_json")
		return
	}
	if files := c.Request.MultipartForm.File["mask"]; len(files) > 0 {
		writeOpenAIError(c, http.StatusBadRequest, "unsupported_parameter", "mask 暂不支持")
		return
	}
	files := legacyMultipartFiles(c.Request.MultipartForm, "image[]", "image")
	if len(files) < 1 || len(files) > 7 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "image 数量必须在 1 到 7 之间")
		return
	}
	imageURLs, err := legacyImageDataURIs(files)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	clientKey, requestID, ok := requestIdentity(c)
	if !ok {
		return
	}
	if !legacyV2Key(clientKey) {
		writeOpenAIError(c, http.StatusUnsupportedMediaType, "invalid_request", "图片编辑仅支持 application/json")
		return
	}
	model = routeModelForClient(clientKey, model)
	result, err := h.gateway.EditImage(c.Request.Context(), gateway.ImageEditInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: model, Prompt: prompt,
		ImageURLs: imageURLs, Count: count, Resolution: "1k", ResponseFormat: responseFormat,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	h.writeResult(c, result, false)
}

func (h *Handler) createLegacyVideo(c *gin.Context) {
	if err := h.parseLegacyMultipart(c); err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	model := strings.TrimSpace(c.PostForm("model"))
	prompt := strings.TrimSpace(c.PostForm("prompt"))
	if model == "" || prompt == "" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "视频生成缺少有效 model 或 prompt")
		return
	}
	duration, err := legacyFormInteger(c.PostForm("seconds"), 6)
	if err != nil || !legacyVideoDuration(duration) {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "seconds 必须是 6、10、12、16 或 20")
		return
	}
	requestedSize := strings.TrimSpace(c.PostForm("size"))
	if requestedSize == "" {
		requestedSize = "720x1280"
	}
	aspectRatio, defaultResolution, ok := legacyVideoSize(requestedSize)
	if !ok {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "size 不受支持")
		return
	}
	resolution := strings.ToLower(strings.TrimSpace(c.PostForm("resolution_name")))
	if resolution == "" {
		resolution = defaultResolution
	}
	if resolution != "480p" && resolution != "720p" {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "resolution_name 必须是 480p 或 720p")
		return
	}
	preset := strings.ToLower(strings.TrimSpace(c.PostForm("preset")))
	if preset == "" {
		preset = "custom"
	}
	if !legacyVideoPreset(preset) {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_parameter", "preset 必须是 fun、normal、spicy 或 custom")
		return
	}
	files := legacyMultipartFiles(c.Request.MultipartForm, "input_reference[]", "input_reference")
	if len(files) > 7 {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", "input_reference 不能超过 7 张")
		return
	}
	referenceURLs, err := legacyImageDataURIs(files)
	if err != nil {
		writeOpenAIError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	clientKey, requestID, ok := requestIdentity(c)
	if !ok {
		return
	}
	if !legacyV2Key(clientKey) {
		writeOpenAIError(c, http.StatusNotFound, "unsupported_endpoint", "Endpoint not found")
		return
	}
	model = routeModelForClient(clientKey, model)
	job, err := h.gateway.CreateVideo(c.Request.Context(), gateway.VideoInput{
		RequestID: requestID, ClientKey: clientKey, PublicModel: model, Prompt: prompt,
		Duration: duration, AspectRatio: aspectRatio, Resolution: resolution,
		ReferenceURLs: referenceURLs, Preset: preset, Legacy: true, LegacySize: requestedSize,
	})
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	c.JSON(http.StatusOK, legacyVideoResponse(job))
}

func (h *Handler) getLegacyVideoContent(c *gin.Context) {
	clientKey, _, ok := requestIdentity(c)
	if !ok {
		return
	}
	job, err := h.gateway.GetVideo(c.Request.Context(), strings.TrimSpace(c.Param("requestId")), clientKey)
	if err != nil {
		writeGatewayError(c, err)
		return
	}
	if !gateway.IsLegacyVideoJob(job) {
		c.Status(http.StatusNotFound)
		return
	}
	status, sourceURL := legacyVideoContent(job)
	if sourceURL == "" {
		writeOpenAIError(c, status, "video_not_ready", "Video content is not ready yet")
		return
	}
	if err := proxyLegacyVideoContent(c, sourceURL, job.ID); err != nil {
		if !c.Writer.Written() {
			writeOpenAIError(c, http.StatusBadGateway, "video_content_unavailable", "Video content is unavailable")
		}
	}
}

func (h *Handler) parseLegacyMultipart(c *gin.Context) error {
	if !strings.HasPrefix(strings.ToLower(c.GetHeader("Content-Type")), "multipart/form-data") {
		return fmt.Errorf("请求必须使用 multipart/form-data")
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, h.maxBodyBytes)
	if err := c.Request.ParseMultipartForm(min(h.maxBodyBytes, legacyMediaFormMemory)); err != nil {
		return fmt.Errorf("multipart 请求无效或超过大小限制")
	}
	return nil
}

func legacyFormInteger(raw string, fallback int) (int, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	return strconv.Atoi(strings.TrimSpace(raw))
}

func legacyMultipartFiles(form *multipart.Form, names ...string) []*multipart.FileHeader {
	if form == nil {
		return nil
	}
	var result []*multipart.FileHeader
	for _, name := range names {
		result = append(result, form.File[name]...)
	}
	return result
}

func legacyImageDataURIs(files []*multipart.FileHeader) ([]string, error) {
	result := make([]string, 0, len(files))
	for index, header := range files {
		file, err := header.Open()
		if err != nil {
			return nil, fmt.Errorf("无法读取第 %d 张图片", index+1)
		}
		data, readErr := io.ReadAll(file)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || len(data) == 0 {
			return nil, fmt.Errorf("无法读取第 %d 张图片", index+1)
		}
		mimeType := http.DetectContentType(data)
		if !strings.HasPrefix(mimeType, "image/") {
			return nil, fmt.Errorf("第 %d 个文件不是有效图片", index+1)
		}
		result = append(result, "data:"+mimeType+";base64,"+base64.StdEncoding.EncodeToString(data))
	}
	return result, nil
}

func legacyVideoDuration(value int) bool {
	switch value {
	case 6, 10, 12, 16, 20:
		return true
	default:
		return false
	}
}

func legacyVideoSize(raw string) (string, string, bool) {
	switch strings.TrimSpace(raw) {
	case "", "720x1280":
		return "9:16", "720p", true
	case "1280x720", "1792x1024":
		return "16:9", "720p", true
	case "1024x1024":
		return "1:1", "720p", true
	case "1024x1792":
		return "9:16", "720p", true
	default:
		return "", "", false
	}
}

func legacyVideoPreset(value string) bool {
	switch value {
	case "fun", "normal", "spicy", "custom":
		return true
	default:
		return false
	}
}

func legacyVideoResponse(job mediadomain.Job) gin.H {
	status := string(job.Status)
	result := gin.H{
		"id": job.ID, "object": "video", "created_at": job.CreatedAt.Unix(),
		"status": status, "model": job.Model, "progress": job.Progress,
		"prompt": job.Prompt, "seconds": strconv.Itoa(job.Seconds),
		"size": legacyVideoResponseSize(job), "quality": "standard",
	}
	if job.CompletedAt != nil {
		result["completed_at"] = job.CompletedAt.Unix()
	}
	if job.Status == mediadomain.StatusFailed {
		result["error"] = gin.H{"code": "video_generation_failed", "message": job.ErrorMessage}
	}
	return result
}

func legacyVideoResponseSize(job mediadomain.Job) string {
	if value := gateway.LegacyVideoSize(job); value != "" {
		return value
	}
	switch job.Size {
	case "16:9":
		return "1280x720"
	case "1:1":
		return "1024x1024"
	default:
		return "720x1280"
	}
}

func legacyVideoContent(job mediadomain.Job) (int, string) {
	if job.Status != mediadomain.StatusCompleted || strings.TrimSpace(job.UpstreamURL) == "" {
		return http.StatusConflict, ""
	}
	return http.StatusOK, job.UpstreamURL
}

func trustedLegacyVideoURL(value string) bool {
	parsed, err := url.Parse(strings.TrimSpace(value))
	if err != nil || parsed.Scheme != "https" || parsed.User != nil {
		return false
	}
	switch strings.ToLower(parsed.Hostname()) {
	case "assets.grok.com", "imagine-public.x.ai":
		return true
	default:
		return false
	}
}

func proxyLegacyVideoContent(c *gin.Context, sourceURL, videoID string) error {
	if !trustedLegacyVideoURL(sourceURL) {
		return fmt.Errorf("视频内容 URL 不受信任")
	}
	request, err := http.NewRequestWithContext(c.Request.Context(), http.MethodGet, sourceURL, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "video/mp4,video/*;q=0.9,*/*;q=0.1")
	client := &http.Client{
		Transport: http.DefaultTransport,
		CheckRedirect: func(request *http.Request, _ []*http.Request) error {
			if !trustedLegacyVideoURL(request.URL.String()) {
				return fmt.Errorf("视频内容重定向到不受信任的 URL")
			}
			return nil
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("视频内容上游返回 %d", response.StatusCode)
	}
	if value := response.Header.Get("Content-Length"); value != "" {
		length, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || length < 0 || length > maxMediaResponseTransferBytes {
			return fmt.Errorf("视频内容长度无效")
		}
		c.Header("Content-Length", value)
	}
	contentType := response.Header.Get("Content-Type")
	if !strings.HasPrefix(strings.ToLower(contentType), "video/") {
		contentType = "video/mp4"
	}
	c.Header("Content-Type", contentType)
	c.Header("Content-Disposition", `attachment; filename="`+videoID+`.mp4"`)
	c.Status(http.StatusOK)
	return copyMedia(responseDeadlineWriter{ResponseWriter: c.Writer}, response.Body, maxMediaResponseTransferBytes)
}
