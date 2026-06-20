package describequorum

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeQuorumResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeQuorumResponseRoundTrip(t *testing.T) {
	in := &DescribeQuorumResponse{
		ErrorCode:    1,
		ErrorMessage: resPtr("x"),
		Topics: &[]DescribeQuorumResponseTopic{DescribeQuorumResponseTopic{
			TopicName: resPtr("x"),
			Partitions: &[]DescribeQuorumResponseTopicPartition{DescribeQuorumResponseTopicPartition{
				PartitionIndex: 1,
				ErrorCode:      1,
				ErrorMessage:   resPtr("x"),
				LeaderId:       1,
				LeaderEpoch:    1,
				HighWatermark:  1,
				CurrentVoters: &[]DescribeQuorumResponseTopicPartitionCurrentVoter{DescribeQuorumResponseTopicPartitionCurrentVoter{
					ReplicaId:             1,
					ReplicaDirectoryId:    uuid.UUID{},
					LogEndOffset:          1,
					LastFetchTimestamp:    1,
					LastCaughtUpTimestamp: 1,
				}},
				Observers: &[]DescribeQuorumResponseTopicPartitionObserver{DescribeQuorumResponseTopicPartitionObserver{
					ReplicaId:             1,
					ReplicaDirectoryId:    uuid.UUID{},
					LogEndOffset:          1,
					LastFetchTimestamp:    1,
					LastCaughtUpTimestamp: 1,
				}},
			}},
		}},
		Nodes: &[]DescribeQuorumResponseNode{DescribeQuorumResponseNode{
			NodeId: 1,
			Listeners: &[]DescribeQuorumResponseNodeListener{DescribeQuorumResponseNodeListener{
				Name: resPtr("x"),
				Host: resPtr("x"),
				Port: 1,
			}},
		}},
	}

	for v := int16(0); v <= 2; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &DescribeQuorumResponse{}
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
		_ = (&DescribeQuorumResponse{}).PrettyPrint()
	}
}
