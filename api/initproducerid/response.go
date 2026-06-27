package initproducerid

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type InitProducerIdResponse struct {
	ApiVersion              int16
	ThrottleTimeMs          int32 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode               int16 // The error code, or 0 if there was no error. (versions: 0+)
	ProducerId              int64 // The current producer id. (versions: 0+)
	ProducerEpoch           int16 // The current epoch associated with the producer id. (versions: 0+)
	OngoingTxnProducerId    int64 // The producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing. (versions: 6+)
	OngoingTxnProducerEpoch int16 // The epoch associated with the  producer id for ongoing transaction when KeepPreparedTxn is used, -1 if there is no transaction ongoing. (versions: 6+)
	rawTaggedFields         *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *InitProducerIdResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ProducerId (versions: 0+)
	if err := protocol.WriteInt64(w, res.ProducerId); err != nil {
		return err
	}

	// ProducerEpoch (versions: 0+)
	if err := protocol.WriteInt16(w, res.ProducerEpoch); err != nil {
		return err
	}

	// OngoingTxnProducerId (versions: 6+)
	if res.ApiVersion >= 6 {
		if err := protocol.WriteInt64(w, res.OngoingTxnProducerId); err != nil {
			return err
		}
	}

	// OngoingTxnProducerEpoch (versions: 6+)
	if res.ApiVersion >= 6 {
		if err := protocol.WriteInt16(w, res.OngoingTxnProducerEpoch); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if res.rawTaggedFields != nil {
			rawTaggedFields = *res.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *InitProducerIdResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("InitProducerIdResponse.Read: response or its body is nil")
	}

	*res = InitProducerIdResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	res.ProducerId = -1
	res.OngoingTxnProducerId = -1
	res.OngoingTxnProducerEpoch = -1

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ProducerId (versions: 0+)
	producerid, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	res.ProducerId = producerid

	// ProducerEpoch (versions: 0+)
	producerepoch, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ProducerEpoch = producerepoch

	// OngoingTxnProducerId (versions: 6+)
	if res.ApiVersion >= 6 {
		ongoingtxnproducerid, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		res.OngoingTxnProducerId = ongoingtxnproducerid
	}

	// OngoingTxnProducerEpoch (versions: 6+)
	if res.ApiVersion >= 6 {
		ongoingtxnproducerepoch, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.OngoingTxnProducerEpoch = ongoingtxnproducerepoch
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *InitProducerIdResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- InitProducerIdResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        ProducerId: %v\n", res.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", res.ProducerEpoch)
	fmt.Fprintf(w, "        OngoingTxnProducerId: %v\n", res.OngoingTxnProducerId)
	fmt.Fprintf(w, "        OngoingTxnProducerEpoch: %v\n", res.OngoingTxnProducerEpoch)

	return w.String()
}
