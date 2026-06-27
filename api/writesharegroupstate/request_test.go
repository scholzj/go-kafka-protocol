package writesharegroupstate

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated WriteShareGroupStateRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestWriteShareGroupStateRequestRoundTrip(t *testing.T) {
	in := &WriteShareGroupStateRequest{
		GroupId: reqPtr("x"),
		Topics: &[]WriteShareGroupStateRequestTopic{WriteShareGroupStateRequestTopic{
			TopicId: uuid.UUID{},
			Partitions: &[]WriteShareGroupStateRequestTopicPartition{WriteShareGroupStateRequestTopicPartition{
				Partition:             1,
				StateEpoch:            1,
				LeaderEpoch:           1,
				StartOffset:           1,
				DeliveryCompleteCount: 1,
				StateBatches: &[]WriteShareGroupStateRequestTopicPartitionStateBatche{WriteShareGroupStateRequestTopicPartitionStateBatche{
					FirstOffset:   1,
					LastOffset:    1,
					DeliveryState: 1,
					DeliveryCount: 1,
				}},
			}},
		}},
	}

	for v := int16(0); v <= 1; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &WriteShareGroupStateRequest{}
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
		_ = (&WriteShareGroupStateRequest{}).PrettyPrint()
	}
}
