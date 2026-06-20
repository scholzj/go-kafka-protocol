package offsetfetch

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated OffsetFetchRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestOffsetFetchRequestRoundTrip(t *testing.T) {
	in := &OffsetFetchRequest{
		GroupId: reqPtr("x"),
		Topics: &[]OffsetFetchRequestTopic{OffsetFetchRequestTopic{
			Name:             reqPtr("x"),
			PartitionIndexes: &[]int32{1},
		}},
		Groups: &[]OffsetFetchRequestGroup{OffsetFetchRequestGroup{
			GroupId:     reqPtr("x"),
			MemberId:    reqPtr("x"),
			MemberEpoch: 1,
			Topics: &[]OffsetFetchRequestGroupTopic{OffsetFetchRequestGroupTopic{
				Name:             reqPtr("x"),
				TopicId:          uuid.UUID{},
				PartitionIndexes: &[]int32{1},
			}},
		}},
		RequireStable: true,
	}

	for v := int16(1); v <= 10; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &OffsetFetchRequest{}
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
		_ = (&OffsetFetchRequest{}).PrettyPrint()
	}
}
