package streamsgroupheartbeat

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated StreamsGroupHeartbeatResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestStreamsGroupHeartbeatResponseRoundTrip(t *testing.T) {
	in := &StreamsGroupHeartbeatResponse{
		ThrottleTimeMs:        1,
		ErrorCode:             1,
		ErrorMessage:          resPtr("x"),
		MemberId:              resPtr("x"),
		MemberEpoch:           1,
		HeartbeatIntervalMs:   1,
		AcceptableRecoveryLag: 1,
		TaskOffsetIntervalMs:  1,
		Status: &[]StreamsGroupHeartbeatResponseStatu{StreamsGroupHeartbeatResponseStatu{
			StatusCode:   1,
			StatusDetail: resPtr("x"),
		}},
		ActiveTasks: &[]StreamsGroupHeartbeatResponseActiveTask{StreamsGroupHeartbeatResponseActiveTask{
			SubtopologyId: resPtr("x"),
			Partitions:    &[]int32{1},
		}},
		StandbyTasks: &[]StreamsGroupHeartbeatResponseStandbyTask{StreamsGroupHeartbeatResponseStandbyTask{
			SubtopologyId: resPtr("x"),
			Partitions:    &[]int32{1},
		}},
		WarmupTasks: &[]StreamsGroupHeartbeatResponseWarmupTask{StreamsGroupHeartbeatResponseWarmupTask{
			SubtopologyId: resPtr("x"),
			Partitions:    &[]int32{1},
		}},
		EndpointInformationEpoch: 1,
		PartitionsByUserEndpoint: &[]StreamsGroupHeartbeatResponsePartitionsByUserEndpoint{StreamsGroupHeartbeatResponsePartitionsByUserEndpoint{
			UserEndpoint: &StreamsGroupHeartbeatResponsePartitionsByUserEndpointUserEndpoint{
				Host: resPtr("x"),
				Port: 1,
			},
			ActivePartitions: &[]StreamsGroupHeartbeatResponsePartitionsByUserEndpointActivePartition{StreamsGroupHeartbeatResponsePartitionsByUserEndpointActivePartition{
				Topic:      resPtr("x"),
				Partitions: &[]int32{1},
			}},
			StandbyPartitions: &[]StreamsGroupHeartbeatResponsePartitionsByUserEndpointStandbyPartition{StreamsGroupHeartbeatResponsePartitionsByUserEndpointStandbyPartition{
				Topic:      resPtr("x"),
				Partitions: &[]int32{1},
			}},
		}},
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &StreamsGroupHeartbeatResponse{
		ThrottleTimeMs:           1,
		ErrorCode:                1,
		ErrorMessage:             nil,
		MemberId:                 resPtr("x"),
		MemberEpoch:              1,
		HeartbeatIntervalMs:      1,
		AcceptableRecoveryLag:    1,
		TaskOffsetIntervalMs:     1,
		Status:                   nil,
		ActiveTasks:              nil,
		StandbyTasks:             nil,
		WarmupTasks:              nil,
		EndpointInformationEpoch: 1,
		PartitionsByUserEndpoint: nil,
	}

	for v := int16(0); v <= 0; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &StreamsGroupHeartbeatResponse{}
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

			out := &StreamsGroupHeartbeatResponse{}
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
		_ = (&StreamsGroupHeartbeatResponse{}).PrettyPrint()
	}
}
