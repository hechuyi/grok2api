package inference

import (
	"bytes"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"strings"
	"testing"
	"time"

	mediadomain "github.com/chenyme/grok2api/backend/internal/domain/media"
	modeldomain "github.com/chenyme/grok2api/backend/internal/domain/model"
	"github.com/gin-gonic/gin"
)

func TestParseLegacyMediaChatExtractsEditAndVideoConfiguration(t *testing.T) {
	body := []byte(`{
		"model":"grok-imagine-image-edit","stream":false,
		"messages":[{"role":"user","content":[
			{"type":"text","text":"make it blue"},
			{"type":"image_url","image_url":{"url":"https://example.com/a.png"}}
		]}],
		"image_config":{"n":2,"size":"1024x1024","response_format":"b64_json"},
		"video_config":{"seconds":20,"size":"1280x720","resolution_name":"720p","preset":"fun"}
	}`)
	request, prompt, images, err := parseLegacyMediaChat(body)
	if err != nil {
		t.Fatal(err)
	}
	if prompt != "make it blue" || len(images) != 1 || images[0] != "https://example.com/a.png" {
		t.Fatalf("prompt=%q images=%#v", prompt, images)
	}
	if request.ImageConfig == nil || request.ImageConfig.Count == nil || *request.ImageConfig.Count != 2 || request.ImageConfig.ResponseFormat != "b64_json" {
		t.Fatalf("image config=%#v", request.ImageConfig)
	}
	if request.VideoConfig == nil || request.VideoConfig.Seconds == nil || *request.VideoConfig.Seconds != 20 || request.VideoConfig.Preset != "fun" {
		t.Fatalf("video config=%#v", request.VideoConfig)
	}
}

func TestLegacyMediaChatDispatchKeepsLiteNativeAndWrapsQualityMedia(t *testing.T) {
	tests := []struct {
		route modeldomain.Route
		want  bool
	}{
		{route: modeldomain.Route{UpstreamModel: "grok-imagine-image-lite", Capability: modeldomain.CapabilityImage}, want: false},
		{route: modeldomain.Route{UpstreamModel: "grok-imagine-image-quality", Capability: modeldomain.CapabilityImage}, want: true},
		{route: modeldomain.Route{Capability: modeldomain.CapabilityImageEdit}, want: true},
		{route: modeldomain.Route{Capability: modeldomain.CapabilityVideo}, want: true},
		{route: modeldomain.Route{Capability: modeldomain.CapabilityChat}, want: false},
	}
	for _, test := range tests {
		if got := legacyMediaChatNeedsDispatch(test.route); got != test.want {
			t.Fatalf("route %#v dispatch=%t, want %t", test.route, got, test.want)
		}
	}
}

func TestLegacyChatCompletionPayloadUsesOpenAIShape(t *testing.T) {
	payload := legacyChatCompletion("grok-imagine-video", "https://assets.grok.com/video.mp4", "视频正在生成 100%")
	if payload["object"] != "chat.completion" || payload["model"] != "grok-imagine-video" {
		t.Fatalf("payload=%#v", payload)
	}
	choices, ok := payload["choices"].([]gin.H)
	if !ok || len(choices) != 1 {
		t.Fatalf("choices=%#v", payload["choices"])
	}
	message, ok := choices[0]["message"].(gin.H)
	if !ok || message["content"] != "https://assets.grok.com/video.mp4" || message["reasoning_content"] != "视频正在生成 100%" {
		t.Fatalf("message=%#v", choices[0]["message"])
	}
}

func TestLegacyImageEditAcceptsMultipartContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandler(nil, nil, 1<<20)

	request := legacyMultipartRequest(t, "/v1/images/edits", map[string]string{
		"model": "grok-imagine-image-edit", "prompt": "make it blue", "n": "2",
		"size": "1024x1024", "response_format": "url",
	}, "image[]", "input.png", "image/png", []byte("\x89PNG\r\n\x1a\nfixture"))
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = request

	handler.editLegacyImage(context)

	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), "invalid_api_key") {
		t.Fatalf("valid multipart image edit status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestLegacyVideoAcceptsMultipartContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	handler := NewHandler(nil, nil, 1<<20)

	request := legacyMultipartRequest(t, "/v1/videos", map[string]string{
		"model": "grok-imagine-video", "prompt": "a comet", "seconds": "20",
		"size": "1280x720", "resolution_name": "720p", "preset": "fun",
	}, "input_reference[]", "reference.jpg", "image/jpeg", []byte("\xff\xd8\xfffixture"))
	recorder := httptest.NewRecorder()
	context, _ := gin.CreateTestContext(recorder)
	context.Request = request

	handler.createLegacyVideo(context)

	if recorder.Code != http.StatusUnauthorized || !strings.Contains(recorder.Body.String(), "invalid_api_key") {
		t.Fatalf("valid multipart video status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestLegacyVideoResponsePreservesV2Shape(t *testing.T) {
	createdAt := time.Unix(1_700_000_000, 0).UTC()
	completedAt := createdAt.Add(time.Minute)
	job := mediadomain.Job{
		ID: "video_123", Model: "grok-imagine-video", Prompt: "a comet", Seconds: 20,
		Size: "16:9", Quality: "720p", Status: mediadomain.StatusCompleted, Progress: 100,
		CreatedAt: createdAt, CompletedAt: &completedAt,
	}

	response := legacyVideoResponse(job)
	if response["id"] != "video_123" || response["object"] != "video" || response["status"] != "completed" {
		t.Fatalf("identity/status response=%#v", response)
	}
	if response["seconds"] != "20" || response["size"] != "1280x720" || response["quality"] != "standard" {
		t.Fatalf("legacy fields response=%#v", response)
	}
	if response["created_at"] != createdAt.Unix() || response["completed_at"] != completedAt.Unix() {
		t.Fatalf("timestamps response=%#v", response)
	}
}

func TestLegacyVideoContentRedirectsOnlyCompletedJobs(t *testing.T) {
	if status, location := legacyVideoContent(mediadomain.Job{Status: mediadomain.StatusInProgress}); status != http.StatusConflict || location != "" {
		t.Fatalf("pending status=%d location=%q", status, location)
	}
	if status, location := legacyVideoContent(mediadomain.Job{Status: mediadomain.StatusCompleted, UpstreamURL: "https://assets.grok.com/video.mp4"}); status != http.StatusOK || location != "https://assets.grok.com/video.mp4" {
		t.Fatalf("completed status=%d location=%q", status, location)
	}
}

func TestLegacyVideoContentOnlyFetchesTrustedAssetHosts(t *testing.T) {
	for _, value := range []string{"https://assets.grok.com/video.mp4", "https://imagine-public.x.ai/video.mp4"} {
		if !trustedLegacyVideoURL(value) {
			t.Fatalf("trusted URL rejected: %s", value)
		}
	}
	for _, value := range []string{"http://assets.grok.com/video.mp4", "https://example.com/video.mp4", "file:///tmp/video.mp4"} {
		if trustedLegacyVideoURL(value) {
			t.Fatalf("untrusted URL accepted: %s", value)
		}
	}
}

func legacyMultipartRequest(t *testing.T, path string, fields map[string]string, fileField, filename, contentType string, content []byte) *http.Request {
	t.Helper()
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	for name, value := range fields {
		if err := writer.WriteField(name, value); err != nil {
			t.Fatal(err)
		}
	}
	header := make(textproto.MIMEHeader)
	header.Set("Content-Disposition", `form-data; name="`+fileField+`"; filename="`+filename+`"`)
	header.Set("Content-Type", contentType)
	part, err := writer.CreatePart(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(content); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, &body)
	request.Header.Set("Content-Type", writer.FormDataContentType())
	return request
}
