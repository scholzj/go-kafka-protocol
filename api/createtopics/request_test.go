package createtopics

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated CreateTopicsRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestCreateTopicsRequestRoundTrip(t *testing.T) {
	in := &CreateTopicsRequest{
		Topics: &[]CreateTopicsRequestTopic{CreateTopicsRequestTopic{
			Name:              reqPtr("x"),
			NumPartitions:     1,
			ReplicationFactor: 1,
			Assignments: &[]CreateTopicsRequestTopicAssignment{CreateTopicsRequestTopicAssignment{
				PartitionIndex: 1,
				BrokerIds:      &[]int32{1},
			}},
			Configs: &[]CreateTopicsRequestTopicConfig{CreateTopicsRequestTopicConfig{
				Name:  reqPtr("x"),
				Value: reqPtr("x"),
			}},
		}},
		TimeoutMs:    1,
		ValidateOnly: true,
	}

	for v := int16(2); v <= 5; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &CreateTopicsRequest{}
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
		_ = (&CreateTopicsRequest{}).PrettyPrint()
	}
}
