package incrementalalterconfigs

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated IncrementalAlterConfigsRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestIncrementalAlterConfigsRequestRoundTrip(t *testing.T) {
	in := &IncrementalAlterConfigsRequest{
		Resources: &[]IncrementalAlterConfigsRequestResource{IncrementalAlterConfigsRequestResource{
			ResourceType: 1,
			ResourceName: reqPtr("x"),
			Configs: &[]IncrementalAlterConfigsRequestResourceConfig{IncrementalAlterConfigsRequestResourceConfig{
				Name:            reqPtr("x"),
				ConfigOperation: 1,
				Value:           reqPtr("x"),
			}},
		}},
		ValidateOnly: true,
	}

	// A second instance with every always-nullable field set to nil. The fully-populated
	// instance never encodes a null, so this is what actually exercises the null-marker
	// write/read paths (nullable single structs, nullable arrays, nullable strings/bytes).
	inNulls := &IncrementalAlterConfigsRequest{
		Resources: &[]IncrementalAlterConfigsRequestResource{IncrementalAlterConfigsRequestResource{
			ResourceType: 1,
			ResourceName: reqPtr("x"),
			Configs: &[]IncrementalAlterConfigsRequestResourceConfig{IncrementalAlterConfigsRequestResourceConfig{
				Name:            reqPtr("x"),
				ConfigOperation: 1,
				Value:           nil,
			}},
		}},
		ValidateOnly: true,
	}

	for v := int16(0); v <= 1; v++ {
		{
			in.ApiVersion = v

			var buf bytes.Buffer
			if err := in.Write(&buf); err != nil {
				t.Fatalf("v%d: populated write: %v", v, err)
			}
			encoded := buf.Bytes()

			out := &IncrementalAlterConfigsRequest{}
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

			out := &IncrementalAlterConfigsRequest{}
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
		_ = (&IncrementalAlterConfigsRequest{}).PrettyPrint()
	}
}
