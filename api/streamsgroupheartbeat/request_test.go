package streamsgroupheartbeat

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated StreamsGroupHeartbeatRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestStreamsGroupHeartbeatRequestRoundTrip(t *testing.T) {
	in := &StreamsGroupHeartbeatRequest{
		GroupId:                  reqPtr("x"),
		MemberId:                 reqPtr("x"),
		MemberEpoch:              1,
		EndpointInformationEpoch: 1,
		InstanceId:               reqPtr("x"),
		RackId:                   reqPtr("x"),
		RebalanceTimeoutMs:       1,
		Topology: &StreamsGroupHeartbeatRequestTopology{
			Epoch: 1,
			Subtopologies: &[]StreamsGroupHeartbeatRequestTopologySubtopologie{StreamsGroupHeartbeatRequestTopologySubtopologie{
				SubtopologyId:    reqPtr("x"),
				SourceTopics:     &[]string{"x"},
				SourceTopicRegex: &[]string{"x"},
				StateChangelogTopics: &[]StreamsGroupHeartbeatRequestTopologySubtopologieStateChangelogTopic{StreamsGroupHeartbeatRequestTopologySubtopologieStateChangelogTopic{
					Name:              reqPtr("x"),
					Partitions:        1,
					ReplicationFactor: 1,
					TopicConfigs: &[]StreamsGroupHeartbeatRequestTopologySubtopologieStateChangelogTopicTopicConfig{StreamsGroupHeartbeatRequestTopologySubtopologieStateChangelogTopicTopicConfig{
						Key:   reqPtr("x"),
						Value: reqPtr("x"),
					}},
				}},
				RepartitionSinkTopics: &[]string{"x"},
				RepartitionSourceTopics: &[]StreamsGroupHeartbeatRequestTopologySubtopologieRepartitionSourceTopic{StreamsGroupHeartbeatRequestTopologySubtopologieRepartitionSourceTopic{
					Name:              reqPtr("x"),
					Partitions:        1,
					ReplicationFactor: 1,
					TopicConfigs: &[]StreamsGroupHeartbeatRequestTopologySubtopologieRepartitionSourceTopicTopicConfig{StreamsGroupHeartbeatRequestTopologySubtopologieRepartitionSourceTopicTopicConfig{
						Key:   reqPtr("x"),
						Value: reqPtr("x"),
					}},
				}},
				CopartitionGroups: &[]StreamsGroupHeartbeatRequestTopologySubtopologieCopartitionGroup{StreamsGroupHeartbeatRequestTopologySubtopologieCopartitionGroup{
					SourceTopics:            &[]int16{1},
					SourceTopicRegex:        &[]int16{1},
					RepartitionSourceTopics: &[]int16{1},
				}},
			}},
		},
		ActiveTasks: &[]StreamsGroupHeartbeatRequestActiveTask{StreamsGroupHeartbeatRequestActiveTask{
			SubtopologyId: reqPtr("x"),
			Partitions:    &[]int32{1},
		}},
		StandbyTasks: &[]StreamsGroupHeartbeatRequestStandbyTask{StreamsGroupHeartbeatRequestStandbyTask{
			SubtopologyId: reqPtr("x"),
			Partitions:    &[]int32{1},
		}},
		WarmupTasks: &[]StreamsGroupHeartbeatRequestWarmupTask{StreamsGroupHeartbeatRequestWarmupTask{
			SubtopologyId: reqPtr("x"),
			Partitions:    &[]int32{1},
		}},
		ProcessId: reqPtr("x"),
		UserEndpoint: &StreamsGroupHeartbeatRequestUserEndpoint{
			Host: reqPtr("x"),
			Port: 1,
		},
		ClientTags: &[]StreamsGroupHeartbeatRequestClientTag{StreamsGroupHeartbeatRequestClientTag{
			Key:   reqPtr("x"),
			Value: reqPtr("x"),
		}},
		TaskOffsets: &[]StreamsGroupHeartbeatRequestTaskOffset{StreamsGroupHeartbeatRequestTaskOffset{
			SubtopologyId: reqPtr("x"),
			Partition:     1,
			Offset:        1,
		}},
		TaskEndOffsets: &[]StreamsGroupHeartbeatRequestTaskEndOffset{StreamsGroupHeartbeatRequestTaskEndOffset{
			SubtopologyId: reqPtr("x"),
			Partition:     1,
			Offset:        1,
		}},
		ShutdownApplication: true,
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &StreamsGroupHeartbeatRequest{
		GroupId:                  reqPtr("x"),
		MemberId:                 reqPtr("x"),
		MemberEpoch:              1,
		EndpointInformationEpoch: 1,
		InstanceId:               nil,
		RackId:                   nil,
		RebalanceTimeoutMs:       1,
		Topology:                 nil,
		ActiveTasks:              nil,
		StandbyTasks:             nil,
		WarmupTasks:              nil,
		ProcessId:                nil,
		UserEndpoint:             nil,
		ClientTags:               nil,
		TaskOffsets:              nil,
		TaskEndOffsets:           nil,
		ShutdownApplication:      true,
	}

	for v := int16(0); v <= 0; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &StreamsGroupHeartbeatRequest{}
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

			out := &StreamsGroupHeartbeatRequest{}
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
		_ = (&StreamsGroupHeartbeatRequest{}).PrettyPrint()
	}
}
