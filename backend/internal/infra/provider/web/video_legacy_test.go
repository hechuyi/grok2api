package web

import (
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

func TestLegacyVideoSegmentsCoverSupportedDurations(t *testing.T) {
	tests := map[int][]int{
		6: {6}, 10: {10}, 12: {6, 6}, 16: {10, 6}, 20: {10, 10},
	}
	for duration, want := range tests {
		if got := videoSegments(duration, true); !reflect.DeepEqual(got, want) {
			t.Fatalf("duration %d segments=%v, want %v", duration, got, want)
		}
	}
	for _, invalid := range []int{0, 1, 11, 15, 21} {
		if got := videoSegments(invalid, true); got != nil {
			t.Fatalf("invalid duration %d segments=%v", invalid, got)
		}
	}
}

func TestVideoSegmentUsesAssetIDWhenPostIDIsAbsent(t *testing.T) {
	fixture := `data: {"result":{"response":{"streamingVideoGenerationResponse":{"progress":100,"assetId":"asset_1","videoUrl":"/videos/final.mp4"}}}}` + "\n"
	response := &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(fixture))}
	_, postID, err := parseVideoStream(response, nil)
	if err != nil {
		t.Fatal(err)
	}
	if postID != "asset_1" {
		t.Fatalf("post ID=%q", postID)
	}
}

func TestVideoExtensionPayloadPreservesSegmentLinkageAndPreset(t *testing.T) {
	payload := videoExtensionPayload("a comet", "parent_1", "segment_1", "16:9", "720p", 6, "fun", 10)
	metadata := payload["responseMetadata"].(map[string]any)
	override := metadata["modelConfigOverride"].(map[string]any)
	modelMap := override["modelMap"].(map[string]any)
	config := modelMap["videoGenModelConfig"].(map[string]any)
	if config["isVideoExtension"] != true || config["extendPostId"] != "segment_1" || config["originalPostId"] != "parent_1" || config["videoExtensionStartTime"] != 10.041667 {
		t.Fatalf("extension config=%#v", config)
	}
	if payload["message"] != "a comet --mode=extremely-crazy" {
		t.Fatalf("message=%q", payload["message"])
	}
}
