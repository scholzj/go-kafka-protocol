package shareacknowledge

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ShareAcknowledgeResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestShareAcknowledgeResponseRoundTrip(t *testing.T) {
	in := &ShareAcknowledgeResponse{
		ThrottleTimeMs:           1,
		ErrorCode:                1,
		ErrorMessage:             resPtr("x"),
		AcquisitionLockTimeoutMs: 1,
		Responses: &[]ShareAcknowledgeResponseResponse{ShareAcknowledgeResponseResponse{
			TopicId: uuid.UUID{},
			Partitions: &[]ShareAcknowledgeResponseResponsePartition{ShareAcknowledgeResponseResponsePartition{
				PartitionIndex: 1,
				ErrorCode:      1,
				ErrorMessage:   resPtr("x"),
				CurrentLeader: &ShareAcknowledgeResponseResponsePartitionCurrentLeader{
					LeaderId:    1,
					LeaderEpoch: 1,
				},
			}},
		}},
		NodeEndpoints: &[]ShareAcknowledgeResponseNodeEndpoint{ShareAcknowledgeResponseNodeEndpoint{
			NodeId: 1,
			Host:   resPtr("x"),
			Port:   1,
			Rack:   resPtr("x"),
		}},
	}

	for v := int16(1); v <= 2; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &ShareAcknowledgeResponse{}
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
		_ = (&ShareAcknowledgeResponse{}).PrettyPrint()
	}
}
