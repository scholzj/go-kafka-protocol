package sharegroupdescribe

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ShareGroupDescribeResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestShareGroupDescribeResponseRoundTrip(t *testing.T) {
	in := &ShareGroupDescribeResponse{
		ThrottleTimeMs: 1,
		Groups: &[]ShareGroupDescribeResponseGroup{ShareGroupDescribeResponseGroup{
			ErrorCode:       1,
			ErrorMessage:    resPtr("x"),
			GroupId:         resPtr("x"),
			GroupState:      resPtr("x"),
			GroupEpoch:      1,
			AssignmentEpoch: 1,
			AssignorName:    resPtr("x"),
			Members: &[]ShareGroupDescribeResponseGroupMember{ShareGroupDescribeResponseGroupMember{
				MemberId:             resPtr("x"),
				RackId:               resPtr("x"),
				MemberEpoch:          1,
				ClientId:             resPtr("x"),
				ClientHost:           resPtr("x"),
				SubscribedTopicNames: &[]string{"x"},
				Assignment: &ShareGroupDescribeResponseGroupMemberAssignment{
					TopicPartitions: &[]ShareGroupDescribeResponseGroupMemberAssignmentTopicPartition{ShareGroupDescribeResponseGroupMemberAssignmentTopicPartition{
						TopicId:    uuid.UUID{},
						TopicName:  resPtr("x"),
						Partitions: &[]int32{1},
					}},
				},
			}},
			AuthorizedOperations: 1,
		}},
	}

	for v := int16(1); v <= 1; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &ShareGroupDescribeResponse{}
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
		_ = (&ShareGroupDescribeResponse{}).PrettyPrint()
	}
}
