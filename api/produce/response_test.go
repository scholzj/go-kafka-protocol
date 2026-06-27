package produce

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func resPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ProduceResponse through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestProduceResponseRoundTrip(t *testing.T) {
	in := &ProduceResponse{
		Responses: &[]ProduceResponseResponse{ProduceResponseResponse{
			Name:    resPtr("x"),
			TopicId: uuid.UUID{},
			PartitionResponses: &[]ProduceResponseResponsePartitionResponse{ProduceResponseResponsePartitionResponse{
				Index:           1,
				ErrorCode:       1,
				BaseOffset:      1,
				LogAppendTimeMs: 1,
				LogStartOffset:  1,
				RecordErrors: &[]ProduceResponseResponsePartitionResponseRecordError{ProduceResponseResponsePartitionResponseRecordError{
					BatchIndex:             1,
					BatchIndexErrorMessage: resPtr("x"),
				}},
				ErrorMessage: resPtr("x"),
				CurrentLeader: &ProduceResponseResponsePartitionResponseCurrentLeader{
					LeaderId:    1,
					LeaderEpoch: 1,
				},
			}},
		}},
		ThrottleTimeMs: 1,
		NodeEndpoints: &[]ProduceResponseNodeEndpoint{ProduceResponseNodeEndpoint{
			NodeId: 1,
			Host:   resPtr("x"),
			Port:   1,
			Rack:   resPtr("x"),
		}},
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &ProduceResponse{
		Responses: &[]ProduceResponseResponse{ProduceResponseResponse{
			Name:    resPtr("x"),
			TopicId: uuid.UUID{},
			PartitionResponses: &[]ProduceResponseResponsePartitionResponse{ProduceResponseResponsePartitionResponse{
				Index:           1,
				ErrorCode:       1,
				BaseOffset:      1,
				LogAppendTimeMs: 1,
				LogStartOffset:  1,
				RecordErrors: &[]ProduceResponseResponsePartitionResponseRecordError{ProduceResponseResponsePartitionResponseRecordError{
					BatchIndex:             1,
					BatchIndexErrorMessage: nil,
				}},
				ErrorMessage: nil,
				CurrentLeader: &ProduceResponseResponsePartitionResponseCurrentLeader{
					LeaderId:    1,
					LeaderEpoch: 1,
				},
			}},
		}},
		ThrottleTimeMs: 1,
		NodeEndpoints: &[]ProduceResponseNodeEndpoint{ProduceResponseNodeEndpoint{
			NodeId: 1,
			Host:   resPtr("x"),
			Port:   1,
			Rack:   nil,
		}},
	}

	for v := int16(3); v <= 13; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &ProduceResponse{}
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

			out := &ProduceResponse{}
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
		_ = (&ProduceResponse{}).PrettyPrint()
	}
}
