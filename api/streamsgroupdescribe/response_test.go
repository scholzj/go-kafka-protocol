package streamsgroupdescribe

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated StreamsGroupDescribeResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestStreamsGroupDescribeResponseRoundTrip(t *testing.T) {
	in := &StreamsGroupDescribeResponse{
		ThrottleTimeMs: 1,
		Groups: &[]StreamsGroupDescribeResponseGroup{StreamsGroupDescribeResponseGroup{
			ErrorCode:       1,
			ErrorMessage:    resPtr("x"),
			GroupId:         resPtr("x"),
			GroupState:      resPtr("x"),
			GroupEpoch:      1,
			AssignmentEpoch: 1,
			Topology: &StreamsGroupDescribeResponseGroupTopology{
				Epoch: 1,
				Subtopologies: &[]StreamsGroupDescribeResponseGroupTopologySubtopologie{StreamsGroupDescribeResponseGroupTopologySubtopologie{
					SubtopologyId:         resPtr("x"),
					SourceTopics:          &[]string{"x"},
					RepartitionSinkTopics: &[]string{"x"},
					StateChangelogTopics: &[]StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic{StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic{
						Name:              resPtr("x"),
						Partitions:        1,
						ReplicationFactor: 1,
						TopicConfigs: &[]StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig{StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig{
							Key:   resPtr("x"),
							Value: resPtr("x"),
						}},
					}},
					RepartitionSourceTopics: &[]StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic{StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic{
						Name:              resPtr("x"),
						Partitions:        1,
						ReplicationFactor: 1,
						TopicConfigs: &[]StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig{StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig{
							Key:   resPtr("x"),
							Value: resPtr("x"),
						}},
					}},
				}},
			},
			Members: &[]StreamsGroupDescribeResponseGroupMember{StreamsGroupDescribeResponseGroupMember{
				MemberId:      resPtr("x"),
				MemberEpoch:   1,
				InstanceId:    resPtr("x"),
				RackId:        resPtr("x"),
				ClientId:      resPtr("x"),
				ClientHost:    resPtr("x"),
				TopologyEpoch: 1,
				ProcessId:     resPtr("x"),
				UserEndpoint: &StreamsGroupDescribeResponseGroupMemberUserEndpoint{
					Host: resPtr("x"),
					Port: 1,
				},
				ClientTags: &[]StreamsGroupDescribeResponseGroupMemberClientTag{StreamsGroupDescribeResponseGroupMemberClientTag{
					Key:   resPtr("x"),
					Value: resPtr("x"),
				}},
				TaskOffsets: &[]StreamsGroupDescribeResponseGroupMemberTaskOffset{StreamsGroupDescribeResponseGroupMemberTaskOffset{
					SubtopologyId: resPtr("x"),
					Partition:     1,
					Offset:        1,
				}},
				TaskEndOffsets: &[]StreamsGroupDescribeResponseGroupMemberTaskEndOffset{StreamsGroupDescribeResponseGroupMemberTaskEndOffset{
					SubtopologyId: resPtr("x"),
					Partition:     1,
					Offset:        1,
				}},
				Assignment: &StreamsGroupDescribeResponseGroupMemberAssignment{
					ActiveTasks: &[]StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask{StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
					StandbyTasks: &[]StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask{StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
					WarmupTasks: &[]StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask{StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
				},
				TargetAssignment: &StreamsGroupDescribeResponseGroupMemberTargetAssignment{
					ActiveTasks: &[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask{StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
					StandbyTasks: &[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask{StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
					WarmupTasks: &[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask{StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask{
						SubtopologyId: resPtr("x"),
						Partitions:    &[]int32{1},
					}},
				},
				IsClassic: true,
			}},
			AuthorizedOperations: 1,
		}},
	}

	for v := int16(0); v <= 0; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &StreamsGroupDescribeResponse{}
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
		_ = (&StreamsGroupDescribeResponse{}).PrettyPrint()
	}
}
