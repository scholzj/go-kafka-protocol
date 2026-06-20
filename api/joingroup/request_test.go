package joingroup

import (
	"bytes"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated JoinGroupRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestJoinGroupRequestRoundTrip(t *testing.T) {
	in := &JoinGroupRequest{
		GroupId:            reqPtr("x"),
		SessionTimeoutMs:   1,
		RebalanceTimeoutMs: 1,
		MemberId:           reqPtr("x"),
		GroupInstanceId:    reqPtr("x"),
		ProtocolType:       reqPtr("x"),
		Protocols: &[]JoinGroupRequestProtocol{JoinGroupRequestProtocol{
			Name:     reqPtr("x"),
			Metadata: &[]byte{1, 2, 3},
		}},
		Reason: reqPtr("x"),
	}

	for v := int16(0); v <= 8; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &JoinGroupRequest{}
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
		_ = (&JoinGroupRequest{}).PrettyPrint()
	}
}
