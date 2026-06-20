package apis

import "testing"

func TestRequestHeaderVersion(t *testing.T) {
	cases := []struct {
		apiKey, apiVersion, want int16
	}{
		{0, 9, 2}, {0, 8, 1}, // Produce (flexible 9+)
		{1, 12, 2}, {1, 11, 1}, // Fetch (flexible 12+)
		{3, 9, 2}, {3, 8, 1}, // Metadata (flexible 9+)
		{10, 3, 2}, {10, 2, 1}, // FindCoordinator (flexible 3+)
		{18, 3, 2}, {18, 2, 1}, // ApiVersions (flexible 3+)
		{60, 0, 2},  // DescribeCluster (flexible 0+)
		{78, 0, 2},  // ShareFetch (flexible 0+)
		{79, 0, 2},  // ShareAcknowledge (flexible 0+)
		{999, 0, 1}, // unknown API -> default header version 1
	}
	for _, c := range cases {
		if got := RequestHeaderVersion(c.apiKey, c.apiVersion); got != c.want {
			t.Errorf("RequestHeaderVersion(%d, %d) = %d, want %d", c.apiKey, c.apiVersion, got, c.want)
		}
	}
}

func TestResponseHeaderVersion(t *testing.T) {
	cases := []struct {
		apiKey, apiVersion, want int16
	}{
		{0, 9, 1}, {0, 8, 0}, // Produce (flexible 9+)
		{1, 12, 1}, {1, 11, 0}, // Fetch (flexible 12+)
		{3, 9, 1}, {3, 8, 0}, // Metadata (flexible 9+)
		{10, 3, 1}, {10, 2, 0}, // FindCoordinator (flexible 3+)
		{18, 3, 0}, {18, 0, 0}, // ApiVersions always uses response header 0
		{60, 0, 1},  // DescribeCluster (flexible 0+)
		{78, 0, 1},  // ShareFetch (flexible 0+)
		{79, 0, 1},  // ShareAcknowledge (flexible 0+)
		{999, 0, 0}, // unknown API -> default header version 0
	}
	for _, c := range cases {
		if got := ResponseHeaderVersion(c.apiKey, c.apiVersion); got != c.want {
			t.Errorf("ResponseHeaderVersion(%d, %d) = %d, want %d", c.apiKey, c.apiVersion, got, c.want)
		}
	}
}
