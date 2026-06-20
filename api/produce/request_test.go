package produce

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ProduceRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestProduceRequestRoundTrip(t *testing.T) {
	in := &ProduceRequest{
		TransactionalId: reqPtr("x"),
		Acks:            1,
		TimeoutMs:       1,
		TopicData: &[]ProduceRequestTopicData{ProduceRequestTopicData{
			Name:    reqPtr("x"),
			TopicId: uuid.UUID{},
			PartitionData: &[]ProduceRequestTopicDataPartitionData{ProduceRequestTopicDataPartitionData{
				Index:   1,
				Records: &[]byte{1, 2, 3},
			}},
		}},
	}

	for v := int16(3); v <= 13; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &ProduceRequest{}
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
		_ = (&ProduceRequest{}).PrettyPrint()
	}
}
