package fetch

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated FetchResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestFetchResponseRoundTrip(t *testing.T) {
	in := &FetchResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		SessionId:      1,
		Responses: &[]FetchResponseResponse{FetchResponseResponse{
			Topic:   resPtr("x"),
			TopicId: uuid.UUID{},
			Partitions: &[]FetchResponseResponsePartition{FetchResponseResponsePartition{
				PartitionIndex:   1,
				ErrorCode:        1,
				HighWatermark:    1,
				LastStableOffset: 1,
				LogStartOffset:   1,
				DivergingEpoch: &FetchResponseResponsePartitionDivergingEpoch{
					Epoch:     1,
					EndOffset: 1,
				},
				CurrentLeader: &FetchResponseResponsePartitionCurrentLeader{
					LeaderId:    1,
					LeaderEpoch: 1,
				},
				SnapshotId: &FetchResponseResponsePartitionSnapshotId{
					EndOffset: 1,
					Epoch:     1,
				},
				AbortedTransactions: &[]FetchResponseResponsePartitionAbortedTransaction{FetchResponseResponsePartitionAbortedTransaction{
					ProducerId:  1,
					FirstOffset: 1,
				}},
				PreferredReadReplica: 1,
				Records:              &[]byte{1, 2, 3},
			}},
		}},
		NodeEndpoints: &[]FetchResponseNodeEndpoint{FetchResponseNodeEndpoint{
			NodeId: 1,
			Host:   resPtr("x"),
			Port:   1,
			Rack:   resPtr("x"),
		}},
	}

	for v := int16(4); v <= 16; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &FetchResponse{}
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
		_ = (&FetchResponse{}).PrettyPrint()
	}
}
