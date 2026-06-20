package allocateproducerids

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AllocateProducerIdsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16 // The top level response error code. (versions: 0+)
	ProducerIdStart int64 // The first producer ID in this range, inclusive. (versions: 0+)
	ProducerIdLen   int32 // The number of producer IDs in this range. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AllocateProducerIdsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ProducerIdStart (versions: 0+)
	if err := protocol.WriteInt64(w, res.ProducerIdStart); err != nil {
		return err
	}

	// ProducerIdLen (versions: 0+)
	if err := protocol.WriteInt32(w, res.ProducerIdLen); err != nil {
		return err
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
func (res *AllocateProducerIdsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AllocateProducerIdsResponse.Read: response or its body is nil")
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

	// ProducerIdStart (versions: 0+)
	produceridstart, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	res.ProducerIdStart = produceridstart

	// ProducerIdLen (versions: 0+)
	produceridlen, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ProducerIdLen = produceridlen

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
func (res *AllocateProducerIdsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AllocateProducerIdsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        ProducerIdStart: %v\n", res.ProducerIdStart)
	fmt.Fprintf(w, "        ProducerIdLen: %v\n", res.ProducerIdLen)

	return w.String()
}
