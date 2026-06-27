package alterpartition

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated AlterPartitionResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestAlterPartitionResponseRoundTrip(t *testing.T) {
	in := &AlterPartitionResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		Topics: &[]AlterPartitionResponseTopic{AlterPartitionResponseTopic{
			TopicId: uuid.UUID{},
			Partitions: &[]AlterPartitionResponseTopicPartition{AlterPartitionResponseTopicPartition{
				PartitionIndex:      1,
				ErrorCode:           1,
				LeaderId:            1,
				LeaderEpoch:         1,
				Isr:                 &[]int32{1},
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

			out := &AlterPartitionResponse{}
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
		_ = (&AlterPartitionResponse{}).PrettyPrint()
	}
}
