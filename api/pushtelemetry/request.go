package pushtelemetry

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type PushTelemetryRequest struct {
	ApiVersion       int16
	ClientInstanceId uuid.UUID // Unique id for this client instance. (versions: 0+)
	SubscriptionId   int32     // Unique identifier for the current subscription. (versions: 0+)
	Terminating      bool      // Client is terminating the connection. (versions: 0+)
	CompressionType  int8      // Compression codec used to compress the metrics. (versions: 0+)
	Metrics          *[]byte   // Metrics encoded in OpenTelemetry MetricsData v1 protobuf format. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *PushTelemetryRequest) Write(w io.Writer) error {
	// ClientInstanceId (versions: 0+)
	if err := protocol.WriteUUID(w, req.ClientInstanceId); err != nil {
		return err
	}

	// SubscriptionId (versions: 0+)
	if err := protocol.WriteInt32(w, req.SubscriptionId); err != nil {
		return err
	}

	// Terminating (versions: 0+)
	if err := protocol.WriteBool(w, req.Terminating); err != nil {
		return err
	}

	// CompressionType (versions: 0+)
	if err := protocol.WriteInt8(w, req.CompressionType); err != nil {
		return err
	}

	// Metrics (versions: 0+)
	if req.Metrics == nil {
		return fmt.Errorf("PushTelemetryRequest.Metrics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *req.Metrics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *req.Metrics); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if req.rawTaggedFields != nil {
			rawTaggedFields = *req.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *PushTelemetryRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("PushTelemetryRequest.Read: request or its body is nil")
	}

	*req = PushTelemetryRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClientInstanceId (versions: 0+)
	clientinstanceid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	req.ClientInstanceId = clientinstanceid

	// SubscriptionId (versions: 0+)
	subscriptionid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.SubscriptionId = subscriptionid

	// Terminating (versions: 0+)
	terminating, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.Terminating = terminating

	// CompressionType (versions: 0+)
	compressiontype, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	req.CompressionType = compressiontype

	// Metrics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		metrics, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		req.Metrics = &metrics
	} else {
		metrics, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		req.Metrics = &metrics
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *PushTelemetryRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> PushTelemetryRequest:\n")
	fmt.Fprintf(w, "        ClientInstanceId: %v\n", req.ClientInstanceId)
	fmt.Fprintf(w, "        SubscriptionId: %v\n", req.SubscriptionId)
	fmt.Fprintf(w, "        Terminating: %v\n", req.Terminating)
	fmt.Fprintf(w, "        CompressionType: %v\n", req.CompressionType)

	if req.Metrics != nil {
		fmt.Fprintf(w, "        Metrics: <%d bytes>\n", len(*req.Metrics))
	} else {
		fmt.Fprintf(w, "        Metrics: nil\n")
	}

	return w.String()
}
