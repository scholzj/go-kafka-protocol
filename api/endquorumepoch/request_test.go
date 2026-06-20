package endquorumepoch

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated EndQuorumEpochRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestEndQuorumEpochRequestRoundTrip(t *testing.T) {
	in := &EndQuorumEpochRequest{
		ClusterId: reqPtr("x"),
		Topics: &[]EndQuorumEpochRequestTopic{EndQuorumEpochRequestTopic{
			TopicName: reqPtr("x"),
			Partitions: &[]EndQuorumEpochRequestTopicPartition{EndQuorumEpochRequestTopicPartition{
				PartitionIndex:      1,
				LeaderId:            1,
				LeaderEpoch:         1,
				PreferredSuccessors: &[]int32{1},
				PreferredCandidates: &[]EndQuorumEpochRequestTopicPartitionPreferredCandidate{EndQuorumEpochRequestTopicPartitionPreferredCandidate{
					CandidateId:          1,
					CandidateDirectoryId: uuid.UUID{},
				}},
			}},
		}},
		LeaderEndpoints: &[]EndQuorumEpochRequestLeaderEndpoint{EndQuorumEpochRequestLeaderEndpoint{
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

		out := &EndQuorumEpochRequest{}
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
		_ = (&EndQuorumEpochRequest{}).PrettyPrint()
	}
}
