package shareacknowledge

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ShareAcknowledgeRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestShareAcknowledgeRequestRoundTrip(t *testing.T) {
	in := &ShareAcknowledgeRequest{
		GroupId:           reqPtr("x"),
		MemberId:          reqPtr("x"),
		ShareSessionEpoch: 1,
		IsRenewAck:        true,
		Topics: &[]ShareAcknowledgeRequestTopic{ShareAcknowledgeRequestTopic{
			TopicId: uuid.UUID{},
			Partitions: &[]ShareAcknowledgeRequestTopicPartition{ShareAcknowledgeRequestTopicPartition{
				PartitionIndex: 1,
				AcknowledgementBatches: &[]ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche{ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche{
					FirstOffset:      1,
					LastOffset:       1,
					AcknowledgeTypes: &[]int8{1},
				}},
			}},
		}},
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &ShareAcknowledgeRequest{
		GroupId:           nil,
		MemberId:          nil,
		ShareSessionEpoch: 1,
		IsRenewAck:        true,
		Topics: &[]ShareAcknowledgeRequestTopic{ShareAcknowledgeRequestTopic{
			TopicId: uuid.UUID{},
			Partitions: &[]ShareAcknowledgeRequestTopicPartition{ShareAcknowledgeRequestTopicPartition{
				PartitionIndex: 1,
				AcknowledgementBatches: &[]ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche{ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche{
					FirstOffset:      1,
					LastOffset:       1,
					AcknowledgeTypes: &[]int8{1},
				}},
			}},
		}},
	}

	for v := int16(1); v <= 2; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &ShareAcknowledgeRequest{}
			request := &protocol.Request{Body: bytes.NewBuffer(encoded)}
			request.ApiVersion = v
			if err := out.Read(request); err != nil {
				t.Fatalf("v%d: populated read: %v", v, err)
			}

			var reencoded bytes.Buffer
			if err := out.Write(&reencoded); err != nil {
				t.Fatalf("v%d: populated re-write: %v", v, err)
			}
			if !bytes.Equal(encoded, reencoded.Bytes()) {
				t.Errorf("v%d: populated round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
			}

			_ = in.PrettyPrint()
		}

		{
			inNulls.ApiVersion = v

			var buf bytes.Buffer
			if err := inNulls.Write(&buf); err != nil {
				t.Fatalf("v%d: nulls write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &ShareAcknowledgeRequest{}
			request := &protocol.Request{Body: bytes.NewBuffer(encoded)}
			request.ApiVersion = v
			if err := out.Read(request); err != nil {
				t.Fatalf("v%d: nulls read: %v", v, err)
			}

			var reencoded bytes.Buffer
			if err := out.Write(&reencoded); err != nil {
				t.Fatalf("v%d: nulls re-write: %v", v, err)
			}
			if !bytes.Equal(encoded, reencoded.Bytes()) {
				t.Errorf("v%d: nulls round-trip mismatch:\n  encoded:   %x\n  reencoded: %x", v, encoded, reencoded.Bytes())
			}

			_ = inNulls.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&ShareAcknowledgeRequest{}).PrettyPrint()
	}
}
