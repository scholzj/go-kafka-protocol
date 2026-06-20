package createacls

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated CreateAclsRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestCreateAclsRequestRoundTrip(t *testing.T) {
	in := &CreateAclsRequest{
		Creations: &[]CreateAclsRequestCreation{CreateAclsRequestCreation{
			ResourceType:        1,
			ResourceName:        reqPtr("x"),
			ResourcePatternType: 1,
			Principal:           reqPtr("x"),
			Host:                reqPtr("x"),
			Operation:           1,
			PermissionType:      1,
		}},
	}

	for v := int16(1); v <= 2; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &CreateAclsRequest{}
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
		_ = (&CreateAclsRequest{}).PrettyPrint()
	}
}
