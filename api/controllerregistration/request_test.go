package controllerregistration

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated ControllerRegistrationRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestControllerRegistrationRequestRoundTrip(t *testing.T) {
	in := &ControllerRegistrationRequest{
		ControllerId:     1,
		IncarnationId:    uuid.UUID{},
		ZkMigrationReady: true,
		Listeners: &[]ControllerRegistrationRequestListener{ControllerRegistrationRequestListener{
			Name:             reqPtr("x"),
			Host:             reqPtr("x"),
			Port:             1,
			SecurityProtocol: 1,
		}},
		Features: &[]ControllerRegistrationRequestFeature{ControllerRegistrationRequestFeature{
			Name:                reqPtr("x"),
			MinSupportedVersion: 1,
			MaxSupportedVersion: 1,
		}},
	}

	for v := int16(0); v <= 0; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &ControllerRegistrationRequest{}
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
		_ = (&ControllerRegistrationRequest{}).PrettyPrint()
	}
}
