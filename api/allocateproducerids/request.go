package allocateproducerids

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AllocateProducerIdsRequest struct {
	ApiVersion      int16
	BrokerId        int32 // The ID of the requesting broker. (versions: 0+)
	BrokerEpoch     int64 // The epoch of the requesting broker. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *AllocateProducerIdsRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
		return err
	}

	// BrokerEpoch (versions: 0+)
	if err := protocol.WriteInt64(w, req.BrokerEpoch); err != nil {
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
func (req *AllocateProducerIdsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AllocateProducerIdsRequest.Read: request or its body is nil")
	}

	*req = AllocateProducerIdsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.BrokerEpoch = -1

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

	// BrokerEpoch (versions: 0+)
	brokerepoch, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.BrokerEpoch = brokerepoch

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
func (req *AllocateProducerIdsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AllocateProducerIdsRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)
	fmt.Fprintf(w, "        BrokerEpoch: %v\n", req.BrokerEpoch)

	return w.String()
}
