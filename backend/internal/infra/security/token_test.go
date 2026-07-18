package security

import "testing"

func TestClientKeyFormat(t *testing.T) {
	raw := FormatClientKey("abc123", "secret_value")
	if raw != "g2a_abc123_secret_value" {
		t.Fatalf("formatted key = %q", raw)
	}
	prefix, ok := SplitClientKey(raw)
	if !ok || prefix != "abc123" {
		t.Fatalf("SplitClientKey(%q) = %q, %v", raw, prefix, ok)
	}
	for _, value := range []string{"", "g2a_", "g2a__secret", "other_abc123_secret", "gbp_abc123_old_secret"} {
		if _, ok := SplitClientKey(value); ok {
			t.Fatalf("SplitClientKey(%q) unexpectedly succeeded", value)
		}
	}
}

func TestClientKeyLookupPrefixSupportsLegacyKeysWithoutWeakeningG2AValidation(t *testing.T) {
	prefix, ok := ClientKeyLookupPrefix("legacy-key-123")
	if !ok || prefix != "legacy_b43642573aa5d4a9" {
		t.Fatalf("legacy prefix = %q, %v", prefix, ok)
	}

	prefix, ok = ClientKeyLookupPrefix("g2a_abc123_secret_value")
	if !ok || prefix != "abc123" {
		t.Fatalf("g2a prefix = %q, %v", prefix, ok)
	}

	for _, value := range []string{"", "g2a_", "g2a__secret", "legacy\nkey"} {
		if _, ok := ClientKeyLookupPrefix(value); ok {
			t.Fatalf("ClientKeyLookupPrefix(%q) unexpectedly succeeded", value)
		}
	}
}
