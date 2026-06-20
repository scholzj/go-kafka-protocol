package txnoffsetcommit

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated TxnOffsetCommitRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestTxnOffsetCommitRequestRoundTrip(t *testing.T) {
	in := &TxnOffsetCommitRequest{
		TransactionalId: reqPtr("x"),
		GroupId:         reqPtr("x"),
		ProducerId:      1,
		ProducerEpoch:   1,
		GenerationId:    1,
		MemberId:        reqPtr("x"),
		GroupInstanceId: reqPtr("x"),
		Topics: &[]TxnOffsetCommitRequestTopic{TxnOffsetCommitRequestTopic{
			Name: reqPtr("x"),
			Partitions: &[]TxnOffsetCommitRequestTopicPartition{TxnOffsetCommitRequestTopicPartition{
				PartitionIndex:       1,
				CommittedOffset:      1,
				CommittedLeaderEpoch: 1,
				CommittedMetadata:    reqPtr("x"),
			}},
		}},
	}

	for v := int16(0); v <= 3; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &TxnOffsetCommitRequest{}
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
		_ = (&TxnOffsetCommitRequest{}).PrettyPrint()
	}
}
