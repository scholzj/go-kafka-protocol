package gettelemetrysubscriptions

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type GetTelemetrySubscriptionsRequest struct {
	ApiVersion       int16
	ClientInstanceId uuid.UUID // Unique id for this client instance, must be set to 0 on the first request. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *GetTelemetrySubscriptionsRequest) Write(w io.Writer) error {
	// ClientInstanceId (versions: 0+)
	if err := protocol.WriteUUID(w, req.ClientInstanceId); err != nil {
		return err
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
func (req *GetTelemetrySubscriptionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("GetTelemetrySubscriptionsRequest.Read: request or its body is nil")
	}

	*req = GetTelemetrySubscriptionsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClientInstanceId (versions: 0+)
	clientinstanceid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	req.ClientInstanceId = clientinstanceid

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
func (req *GetTelemetrySubscriptionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> GetTelemetrySubscriptionsRequest:\n")
	fmt.Fprintf(w, "        ClientInstanceId: %v\n", req.ClientInstanceId)

	return w.String()
}
