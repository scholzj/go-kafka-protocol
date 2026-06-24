package messages

import "testing"

// These tests cover the hand-maintained contract of the generated registry: that the four lookup
// tables agree with each other and that the named constants resolve to the structs/names we expect.

func TestKnownApiKeysAreConsistent(t *testing.T) {
	// Spot-check a handful of well-known APIs across all four tables.
	cases := []struct {
		apiKey int16
		name   string
	}{
		{Produce, "Produce"},
		{Fetch, "Fetch"},
		{Metadata, "Metadata"},
		{ApiVersions, "ApiVersions"},
		{FindCoordinator, "FindCoordinator"},
		{DescribeCluster, "DescribeCluster"},
	}

	for _, c := range cases {
		if got := Name(c.apiKey); got != c.name {
			t.Errorf("Name(%d) = %q, want %q", c.apiKey, got, c.name)
		}

		if _, ok := NewRequestBody(c.apiKey); !ok {
			t.Errorf("NewRequestBody(%d): no request body registered", c.apiKey)
		}
		if _, ok := NewResponseBody(c.apiKey); !ok {
			t.Errorf("NewResponseBody(%d): no response body registered", c.apiKey)
		}

		minV, maxV, ok := VersionRange(c.apiKey)
		if !ok {
			t.Errorf("VersionRange(%d): not registered", c.apiKey)
		}
		if minV > maxV {
			t.Errorf("VersionRange(%d) = (%d,%d): min must not exceed max", c.apiKey, minV, maxV)
		}
	}
}

func TestConstructorsReturnFreshUsableBodies(t *testing.T) {
	// A registered constructor must return a non-nil body that satisfies the interface and is a
	// fresh instance each call (so concurrent connections never share decode state).
	req1, ok := NewRequestBody(Metadata)
	if !ok || req1 == nil {
		t.Fatalf("NewRequestBody(Metadata) = (%v, %v)", req1, ok)
	}
	req2, _ := NewRequestBody(Metadata)
	if req1 == req2 {
		t.Error("NewRequestBody returned the same instance twice; must allocate fresh structs")
	}

	resp, ok := NewResponseBody(Metadata)
	if !ok || resp == nil {
		t.Fatalf("NewResponseBody(Metadata) = (%v, %v)", resp, ok)
	}
	// PrettyPrint must be callable on a zero-valued body without panicking.
	_ = resp.PrettyPrint()
}

func TestUnknownApiKey(t *testing.T) {
	const unknown int16 = 30000

	if _, ok := NewRequestBody(unknown); ok {
		t.Error("NewRequestBody(unknown): expected ok=false")
	}
	if _, ok := NewResponseBody(unknown); ok {
		t.Error("NewResponseBody(unknown): expected ok=false")
	}
	if got := Name(unknown); got != "Unknown" {
		t.Errorf("Name(unknown) = %q, want \"Unknown\"", got)
	}
	if _, _, ok := VersionRange(unknown); ok {
		t.Error("VersionRange(unknown): expected ok=false")
	}
}
