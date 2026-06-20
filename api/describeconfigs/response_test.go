package describeconfigs

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeConfigsResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeConfigsResponseRoundTrip(t *testing.T) {
	in := &DescribeConfigsResponse{
		ThrottleTimeMs: 1,
		Results: &[]DescribeConfigsResponseResult{DescribeConfigsResponseResult{
			ErrorCode:    1,
			ErrorMessage: resPtr("x"),
			ResourceType: 1,
			ResourceName: resPtr("x"),
			Configs: &[]DescribeConfigsResponseResultConfig{DescribeConfigsResponseResultConfig{
				Name:         resPtr("x"),
				Value:        resPtr("x"),
				ReadOnly:     true,
				ConfigSource: 1,
				IsSensitive:  true,
				Synonyms: &[]DescribeConfigsResponseResultConfigSynonym{DescribeConfigsResponseResultConfigSynonym{
					Name:   resPtr("x"),
					Value:  resPtr("x"),
					Source: 1,
				}},
				ConfigType:    1,
				Documentation: resPtr("x"),
			}},
		}},
	}

	for v := int16(1); v <= 4; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &DescribeConfigsResponse{}
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
		_ = (&DescribeConfigsResponse{}).PrettyPrint()
	}
}
