package gateway

import (
	"slices"
	"testing"

	"github.com/chenyme/grok2api/backend/internal/domain/media"
)

func TestLegacyVideoJobInputRoundTripsProtocolReferencesAndPreset(t *testing.T) {
	encoded := encodeVideoInput([]string{"data:image/png;base64,AA=="}, "fun", true, "1792x1024")
	input := decodeVideoJobInput(encoded)
	if !input.Legacy || input.Preset != "fun" || input.LegacySize != "1792x1024" || !slices.Equal(input.ReferenceURLs, []string{"data:image/png;base64,AA=="}) {
		t.Fatalf("decoded input=%#v", input)
	}
	job := media.Job{InputJSON: encoded}
	if !IsLegacyVideoJob(job) {
		t.Fatal("legacy job was not recognized")
	}
	if LegacyVideoSize(job) != "1792x1024" {
		t.Fatalf("legacy size=%q", LegacyVideoSize(job))
	}
}

func TestExistingVideoJobInputRemainsCompatible(t *testing.T) {
	input := decodeVideoJobInput(`{"image_urls":["https://example.com/reference.png"]}`)
	if input.Legacy || input.Preset != "" || !slices.Equal(input.ReferenceURLs, []string{"https://example.com/reference.png"}) {
		t.Fatalf("decoded existing input=%#v", input)
	}
}
