package consumergroupdescribe

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ConsumerGroupDescribeResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestConsumerGroupDescribeResponseRoundTrip(t *testing.T) {
	in := &ConsumerGroupDescribeResponse{
		ThrottleTimeMs: 1,
		Groups: &[]ConsumerGroupDescribeResponseGroup{ConsumerGroupDescribeResponseGroup{
			ErrorCode:       1,
			ErrorMessage:    resPtr("x"),
			GroupId:         resPtr("x"),
			GroupState:      resPtr("x"),
			GroupEpoch:      1,
			AssignmentEpoch: 1,
			AssignorName:    resPtr("x"),
			Members: &[]ConsumerGroupDescribeResponseGroupMember{ConsumerGroupDescribeResponseGroupMember{
				MemberId:             resPtr("x"),
				InstanceId:           resPtr("x"),
				RackId:               resPtr("x"),
				MemberEpoch:          1,
				ClientId:             resPtr("x"),
				ClientHost:           resPtr("x"),
				SubscribedTopicNames: &[]string{"x"},
				SubscribedTopicRegex: resPtr("x"),
				Assignment: &ConsumerGroupDescribeResponseGroupMemberAssignment{
					TopicPartitions: &[]ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition{ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition{
						TopicId:    uuid.UUID{},
						TopicName:  resPtr("x"),
						Partitions: &[]int32{1},
					}},
				},
				TargetAssignment: &ConsumerGroupDescribeResponseGroupMemberTargetAssignment{
					TopicPartitions: &[]ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition{ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition{
						TopicId:    uuid.UUID{},
						TopicName:  resPtr("x"),
						Partitions: &[]int32{1},
					}},
				},
				MemberType: 1,
			}},
			AuthorizedOperations: 1,
		}},
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &ConsumerGroupDescribeResponse{
		ThrottleTimeMs: 1,
		Groups: &[]ConsumerGroupDescribeResponseGroup{ConsumerGroupDescribeResponseGroup{
			ErrorCode:       1,
			ErrorMessage:    nil,
			GroupId:         resPtr("x"),
			GroupState:      resPtr("x"),
			GroupEpoch:      1,
			AssignmentEpoch: 1,
			AssignorName:    resPtr("x"),
			Members: &[]ConsumerGroupDescribeResponseGroupMember{ConsumerGroupDescribeResponseGroupMember{
				MemberId:             resPtr("x"),
				InstanceId:           nil,
				RackId:               nil,
				MemberEpoch:          1,
				ClientId:             resPtr("x"),
				ClientHost:           resPtr("x"),
				SubscribedTopicNames: &[]string{"x"},
				SubscribedTopicRegex: nil,
				Assignment: &ConsumerGroupDescribeResponseGroupMemberAssignment{
					TopicPartitions: &[]ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition{ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition{
						TopicId:    uuid.UUID{},
						TopicName:  resPtr("x"),
						Partitions: &[]int32{1},
					}},
				},
				TargetAssignment: &ConsumerGroupDescribeResponseGroupMemberTargetAssignment{
					TopicPartitions: &[]ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition{ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition{
						TopicId:    uuid.UUID{},
						TopicName:  resPtr("x"),
						Partitions: &[]int32{1},
					}},
				},
				MemberType: 1,
			}},
			AuthorizedOperations: 1,
		}},
	}

	for v := int16(0); v <= 1; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &ConsumerGroupDescribeResponse{}
			response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
			response.ApiVersion = v
			if err := out.Read(response); err != nil {
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

			out := &ConsumerGroupDescribeResponse{}
			response := &protocol.Response{Body: bytes.NewBuffer(encoded)}
			response.ApiVersion = v
			if err := out.Read(response); err != nil {
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
		_ = (&ConsumerGroupDescribeResponse{}).PrettyPrint()
	}
}
