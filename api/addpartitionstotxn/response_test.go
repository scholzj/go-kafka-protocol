package addpartitionstotxn

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated AddPartitionsToTxnResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestAddPartitionsToTxnResponseRoundTrip(t *testing.T) {
	in := &AddPartitionsToTxnResponse{
		ThrottleTimeMs: 1,
		ErrorCode:      1,
		ResultsByTransaction: &[]AddPartitionsToTxnResponseResultsByTransaction{AddPartitionsToTxnResponseResultsByTransaction{
			TransactionalId: resPtr("x"),
			TopicResults: &[]AddPartitionsToTxnResponseResultsByTransactionTopicResult{AddPartitionsToTxnResponseResultsByTransactionTopicResult{
				Name: resPtr("x"),
				ResultsByPartition: &[]AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition{AddPartitionsToTxnResponseResultsByTransactionTopicResultResultsByPartition{
					PartitionIndex:     1,
					PartitionErrorCode: 1,
				}},
			}},
		}},
		ResultsByTopicV3AndBelow: &[]AddPartitionsToTxnResponseResultsByTopicV3AndBelow{AddPartitionsToTxnResponseResultsByTopicV3AndBelow{
			Name: resPtr("x"),
			ResultsByPartition: &[]AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition{AddPartitionsToTxnResponseResultsByTopicV3AndBelowResultsByPartition{
				PartitionIndex:     1,
				PartitionErrorCode: 1,
			}},
		}},
	}

	for v := int16(0); v <= 4; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &AddPartitionsToTxnResponse{}
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
		_ = (&AddPartitionsToTxnResponse{}).PrettyPrint()
	}
}
