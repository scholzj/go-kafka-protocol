package describetransactions

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated DescribeTransactionsResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestDescribeTransactionsResponseRoundTrip(t *testing.T) {
	in := &DescribeTransactionsResponse{
		ThrottleTimeMs: 1,
		TransactionStates: &[]DescribeTransactionsResponseTransactionState{DescribeTransactionsResponseTransactionState{
			ErrorCode:              1,
			TransactionalId:        resPtr("x"),
			TransactionState:       resPtr("x"),
			TransactionTimeoutMs:   1,
			TransactionStartTimeMs: 1,
			ProducerId:             1,
			ProducerEpoch:          1,
			Topics: &[]DescribeTransactionsResponseTransactionStateTopic{DescribeTransactionsResponseTransactionStateTopic{
				Topic:      resPtr("x"),
				Partitions: &[]int32{1},
			}},
		}},
	}

	for v := int16(0); v <= 0; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &DescribeTransactionsResponse{}
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

			_ = in.PrettyPrint()
		}

		// PrettyPrint must not panic on the zero value either.
		_ = (&DescribeTransactionsResponse{}).PrettyPrint()
	}
}
