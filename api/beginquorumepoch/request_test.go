package beginquorumepoch

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated BeginQuorumEpochRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestBeginQuorumEpochRequestRoundTrip(t *testing.T) {
	in := &BeginQuorumEpochRequest{
		ClusterId: reqPtr("x"),
		VoterId:   1,
		Topics: &[]BeginQuorumEpochRequestTopic{BeginQuorumEpochRequestTopic{
			TopicName: reqPtr("x"),
			Partitions: &[]BeginQuorumEpochRequestTopicPartition{BeginQuorumEpochRequestTopicPartition{
				PartitionIndex:   1,
				VoterDirectoryId: uuid.UUID{},
				LeaderId:         1,
				LeaderEpoch:      1,
			}},
		}},
		LeaderEndpoints: &[]BeginQuorumEpochRequestLeaderEndpoint{BeginQuorumEpochRequestLeaderEndpoint{
			Name: reqPtr("x"),
			Host: reqPtr("x"),
			Port: 1,
		}},
	}

	for v := int16(0); v <= 1; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &BeginQuorumEpochRequest{}
		request := &protocol.Request{Body: bytes.NewBuffer(encoded)}
		request.ApiVersion = v
		if err := out.Read(request); err != nil {
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
		_ = (&BeginQuorumEpochRequest{}).PrettyPrint()
	}
}
