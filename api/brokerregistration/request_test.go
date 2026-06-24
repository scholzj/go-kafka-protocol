package brokerregistration

import (
	"bytes"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"testing"
)

func reqPtr[T any](v T) *T { return &v }

// Round-trips a fully populated BrokerRegistrationRequest through Write/Read/Write at every
// valid protocol version and checks the re-encoded bytes match.
func TestBrokerRegistrationRequestRoundTrip(t *testing.T) {
	in := &BrokerRegistrationRequest{
		BrokerId:      1,
		ClusterId:     reqPtr("x"),
		IncarnationId: uuid.UUID{},
		Listeners: &[]BrokerRegistrationRequestListener{BrokerRegistrationRequestListener{
			Name:             reqPtr("x"),
			Host:             reqPtr("x"),
			Port:             1,
			SecurityProtocol: 1,
		}},
		Features: &[]BrokerRegistrationRequestFeature{BrokerRegistrationRequestFeature{
			Name:                reqPtr("x"),
			MinSupportedVersion: 1,
			MaxSupportedVersion: 1,
		}},
		Rack:                reqPtr("x"),
		IsMigratingZkBroker: true,
		LogDirs:             &[]uuid.UUID{uuid.UUID{}},
		PreviousBrokerEpoch: 1,
	}

	for v := int16(0); v <= 3; v++ {
		in.ApiVersion = v

		var buf bytes.Buffer
		if err := in.Write(&buf); err != nil {
			t.Fatalf("v%d: write: %v", v, err)
		}
		encoded := buf.Bytes()

		out := &BrokerRegistrationRequest{}
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
		_ = (&BrokerRegistrationRequest{}).PrettyPrint()
	}
}
