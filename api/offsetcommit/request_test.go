package offsetcommit

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated OffsetCommitRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestOffsetCommitRequestRoundTrip(t *testing.T) {
	in := &OffsetCommitRequest{
		GroupId:                   reqPtr("x"),
		GenerationIdOrMemberEpoch: 1,
		MemberId:                  reqPtr("x"),
		GroupInstanceId:           reqPtr("x"),
		RetentionTimeMs:           1,
		Topics: &[]OffsetCommitRequestTopic{OffsetCommitRequestTopic{
			Name:    reqPtr("x"),
			TopicId: uuid.UUID{},
			Partitions: &[]OffsetCommitRequestTopicPartition{OffsetCommitRequestTopicPartition{
				PartitionIndex:       1,
				CommittedOffset:      1,
				CommittedLeaderEpoch: 1,
				CommittedMetadata:    reqPtr("x"),
			}},
		}},
	}

	for v := int16(2); v <= 10; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &OffsetCommitRequest{}
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
		_ = (&OffsetCommitRequest{}).PrettyPrint()
	}
}
