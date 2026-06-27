package envelope

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EnvelopeResponse struct {
	ApiVersion      int16
	ResponseData    *[]byte // The embedded response header and data. (versions: 0+, nullable: 0+)
	ErrorCode       int16   // The error code, or 0 if there was no error. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *EnvelopeResponse) Write(w io.Writer) error {
	// ResponseData (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactBytes(w, res.ResponseData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableBytes(w, res.ResponseData); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
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
func (res *EnvelopeResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("EnvelopeResponse.Read: response or its body is nil")
	}

	*res = EnvelopeResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ResponseData (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		responsedata, err := protocol.ReadNullableCompactBytes(r)
		if err != nil {
			return err
		}
		res.ResponseData = responsedata
	} else {
		responsedata, err := protocol.ReadNullableBytes(r)
		if err != nil {
			return err
		}
		res.ResponseData = responsedata
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

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
func (res *EnvelopeResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- EnvelopeResponse:\n")

	if res.ResponseData != nil {
		fmt.Fprintf(w, "        ResponseData: <%d bytes>\n", len(*res.ResponseData))
	} else {
		fmt.Fprintf(w, "        ResponseData: nil\n")
	}

	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	return w.String()
}
