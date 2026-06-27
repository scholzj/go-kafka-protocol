package alterpartition

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated AlterPartitionRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestAlterPartitionRequestRoundTrip(t *testing.T) {
	in := &AlterPartitionRequest{
		BrokerId:    1,
		BrokerEpoch: 1,
		Topics: &[]AlterPartitionRequestTopic{AlterPartitionRequestTopic{
			TopicId: uuid.UUID{},
			Partitions: &[]AlterPartitionRequestTopicPartition{AlterPartitionRequestTopicPartition{
				PartitionIndex: 1,
				LeaderEpoch:    1,
				NewIsr:         &[]int32{1},
				NewIsrWithEpochs: &[]AlterPartitionRequestTopicPartitionNewIsrWithEpoch{AlterPartitionRequestTopicPartitionNewIsrWithEpoch{
					BrokerId:    1,
					BrokerEpoch: 1,
				}},
				LeaderRecoveryState: 1,
				PartitionEpoch:      1,
			}},
		}},
	}

	for v := int16(2); v <= 3; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &AlterPartitionRequest{}
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

			_ = in.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&AlterPartitionRequest{}).PrettyPrint()
	}
}
