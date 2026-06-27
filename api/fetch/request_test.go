package fetch

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated FetchRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestFetchRequestRoundTrip(t *testing.T) {
	in := &FetchRequest{
		ClusterId: reqPtr("x"),
		ReplicaId: 1,
		ReplicaState: &FetchRequestReplicaState{
			ReplicaId:    1,
			ReplicaEpoch: 1,
		},
		MaxWaitMs:      1,
		MinBytes:       1,
		MaxBytes:       1,
		IsolationLevel: 1,
		SessionId:      1,
		SessionEpoch:   1,
		Topics: &[]FetchRequestTopic{FetchRequestTopic{
			Topic:   reqPtr("x"),
			TopicId: uuid.UUID{},
			Partitions: &[]FetchRequestTopicPartition{FetchRequestTopicPartition{
				Partition:          1,
				CurrentLeaderEpoch: 1,
				FetchOffset:        1,
				LastFetchedEpoch:   1,
				LogStartOffset:     1,
				PartitionMaxBytes:  1,
				ReplicaDirectoryId: uuid.UUID{},
				HighWatermark:      1,
			}},
		}},
		ForgottenTopicsData: &[]FetchRequestForgottenTopicsData{FetchRequestForgottenTopicsData{
			Topic:      reqPtr("x"),
			TopicId:    uuid.UUID{},
			Partitions: &[]int32{1},
		}},
		RackId: reqPtr("x"),
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &FetchRequest{
		ClusterId: nil,
		ReplicaId: 1,
		ReplicaState: &FetchRequestReplicaState{
			ReplicaId:    1,
			ReplicaEpoch: 1,
		},
		MaxWaitMs:      1,
		MinBytes:       1,
		MaxBytes:       1,
		IsolationLevel: 1,
		SessionId:      1,
		SessionEpoch:   1,
		Topics: &[]FetchRequestTopic{FetchRequestTopic{
			Topic:   reqPtr("x"),
			TopicId: uuid.UUID{},
			Partitions: &[]FetchRequestTopicPartition{FetchRequestTopicPartition{
				Partition:          1,
				CurrentLeaderEpoch: 1,
				FetchOffset:        1,
				LastFetchedEpoch:   1,
				LogStartOffset:     1,
				PartitionMaxBytes:  1,
				ReplicaDirectoryId: uuid.UUID{},
				HighWatermark:      1,
			}},
		}},
		ForgottenTopicsData: &[]FetchRequestForgottenTopicsData{FetchRequestForgottenTopicsData{
			Topic:      reqPtr("x"),
			TopicId:    uuid.UUID{},
			Partitions: &[]int32{1},
		}},
		RackId: reqPtr("x"),
	}

	for v := int16(4); v <= 18; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &FetchRequest{}
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

			out := &FetchRequest{}
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
		_ = (&FetchRequest{}).PrettyPrint()
	}
}
