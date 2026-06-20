package listconfigresources

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ListConfigResourcesResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestListConfigResourcesResponseRoundTrip(t *testing.T) {
	in := &ListConfigResourcesResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ConfigResources: &[]ListConfigResourcesResponseConfigResource{ListConfigResourcesResponseConfigResource{
			ResourceName: resPtr("x"),
			ResourceType: 1,
		}},
	}

	for v := int16(0); v <= 1; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &ListConfigResourcesResponse{}
		response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
		response.ApiVersion = v
		if err := out.Read(response); err != nil {
			t.Fatalf("v%d: read: %v", v, err)
		}

		var reencoded bytes.Buffer
		if err := out.Write(&reencoded); err != nil {
			t.Fatalf("v%d: re-write: %v", v, err)
		}
		if !bytes.Equal(encoded, reencoded.Bytes()) {
			t.Errorf("v%d: round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
		}

		// PrettyPrint must not panic, for the populated and the zero value alike.
		_ = in.PrettyPrint()
		_ = (&ListConfigResourcesResponse{}).PrettyPrint()
	}
}
