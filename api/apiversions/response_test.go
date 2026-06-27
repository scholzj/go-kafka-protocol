package apiversions

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ApiVersionsResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestApiVersionsResponseRoundTrip(t *testing.T) {
	in := &ApiVersionsResponse{
		ErrorCode: 1,
		ApiKeys: &[]ApiVersionsResponseApiKey{ApiVersionsResponseApiKey{
			ApiKey:     1,
			MinVersion: 1,
			MaxVersion: 1,
		}},
		ThrottleTimeMs: 1,
		SupportedFeatures: &[]ApiVersionsResponseSupportedFeature{ApiVersionsResponseSupportedFeature{
			Name:       resPtr("x"),
			MinVersion: 1,
			MaxVersion: 1,
		}},
		FinalizedFeaturesEpoch: 1,
		FinalizedFeatures: &[]ApiVersionsResponseFinalizedFeature{ApiVersionsResponseFinalizedFeature{
			Name:            resPtr("x"),
			MaxVersionLevel: 1,
			MinVersionLevel: 1,
		}},
		ZkMigrationReady: true,
	}

	for v := int16(0); v <= 4; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &ApiVersionsResponse{}
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

			_ = in.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&ApiVersionsResponse{}).PrettyPrint()
	}
}
