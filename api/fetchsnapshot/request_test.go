package fetchsnapshot

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated FetchSnapshotRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestFetchSnapshotRequestRoundTrip(t *testing.T) {
	in := &FetchSnapshotRequest{
		ClusterId: reqPtr("x"),
		ReplicaId: 1,
		MaxBytes:  1,
		Topics: &[]FetchSnapshotRequestTopic{FetchSnapshotRequestTopic{
			Name: reqPtr("x"),
			Partitions: &[]FetchSnapshotRequestTopicPartition{FetchSnapshotRequestTopicPartition{
				Partition:          1,
				CurrentLeaderEpoch: 1,
				SnapshotId: &FetchSnapshotRequestTopicPartitionSnapshotId{
					EndOffset: 1,
					Epoch:     1,
				},
				Position:           1,
				ReplicaDirectoryId: uuid.UUID{},
			}},
		}},
	}

	for v := int16(0); v <= 1; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &FetchSnapshotRequest{}
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
		_ = (&FetchSnapshotRequest{}).PrettyPrint()
	}
}
