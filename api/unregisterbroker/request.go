package unregisterbroker

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type UnregisterBrokerRequest struct {
	ApiVersion      int16
	BrokerId        int32 // The broker ID to unregister. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *UnregisterBrokerRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
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
func (req *UnregisterBrokerRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("UnregisterBrokerRequest.Read: request or its body is nil")
	}

	*req = UnregisterBrokerRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

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
func (req *UnregisterBrokerRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> UnregisterBrokerRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)

	return w.String()
}
