package endtxn

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EndTxnResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16 // The error code, or 0 if there was no error. (versions: 0+)
	ProducerId      int64 // The producer ID. (versions: 5+)
	ProducerEpoch   int16 // The current epoch associated with the producer. (versions: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (res *EndTxnResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ProducerId (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt64(w, res.ProducerId); err != nil {
			return err
		}
	}

	// ProducerEpoch (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteInt16(w, res.ProducerEpoch); err != nil {
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
func (res *EndTxnResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("EndTxnResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

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

	// ProducerId (versions: 5+)
	if res.ApiVersion >= 5 {
		producerid, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		res.ProducerId = producerid
	}

	// ProducerEpoch (versions: 5+)
	if res.ApiVersion >= 5 {
		producerepoch, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ProducerEpoch = producerepoch
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
func (res *EndTxnResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- EndTxnResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        ProducerId: %v\n", res.ProducerId)
	fmt.Fprintf(w, "        ProducerEpoch: %v\n", res.ProducerEpoch)

	return w.String()
}
