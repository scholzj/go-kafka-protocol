package incrementalalterconfigs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type IncrementalAlterConfigsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                      // Duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Responses       *[]IncrementalAlterConfigsResponseResponse // The responses for each resource. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type IncrementalAlterConfigsResponseResponse struct {
	ErrorCode       int16   // The resource error code. (versions: 0+)
	ErrorMessage    *string // The resource error message, or null if there was no error. (versions: 0+, nullable: 0+)
	ResourceType    int8    // The resource type. (versions: 0+)
	ResourceName    *string // The resource name. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (res *IncrementalAlterConfigsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Responses (versions: 0+)
	if res.Responses == nil {
		return fmt.Errorf("IncrementalAlterConfigsResponse.Responses must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.responsesEncoder, res.Responses); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.responsesEncoder, *res.Responses); err != nil {
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
func (res *IncrementalAlterConfigsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("IncrementalAlterConfigsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Responses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		responses, err := protocol.ReadNullableCompactArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = responses
	} else {
		responses, err := protocol.ReadArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = &responses
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

func (res *IncrementalAlterConfigsResponse) responsesEncoder(w io.Writer, value IncrementalAlterConfigsResponseResponse) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
			return err
		}
	}

	// ResourceType (versions: 0+)
	if err := protocol.WriteInt8(w, value.ResourceType); err != nil {
		return err
	}

	// ResourceName (versions: 0+)
	if value.ResourceName == nil {
		return fmt.Errorf("IncrementalAlterConfigsResponseResponse.ResourceName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ResourceName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ResourceName); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if value.rawTaggedFields != nil {
			rawTaggedFields = *value.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *IncrementalAlterConfigsResponse) responsesDecoder(r io.Reader) (IncrementalAlterConfigsResponseResponse, error) {
	incrementalalterconfigsresponseresponse := IncrementalAlterConfigsResponseResponse{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return incrementalalterconfigsresponseresponse, err
	}
	incrementalalterconfigsresponseresponse.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return incrementalalterconfigsresponseresponse, err
		}
		incrementalalterconfigsresponseresponse.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return incrementalalterconfigsresponseresponse, err
		}
		incrementalalterconfigsresponseresponse.ErrorMessage = errormessage
	}

	// ResourceType (versions: 0+)
	resourcetype, err := protocol.ReadInt8(r)
	if err != nil {
		return incrementalalterconfigsresponseresponse, err
	}
	incrementalalterconfigsresponseresponse.ResourceType = resourcetype

	// ResourceName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		resourcename, err := protocol.ReadCompactString(r)
		if err != nil {
			return incrementalalterconfigsresponseresponse, err
		}
		incrementalalterconfigsresponseresponse.ResourceName = &resourcename
	} else {
		resourcename, err := protocol.ReadString(r)
		if err != nil {
			return incrementalalterconfigsresponseresponse, err
		}
		incrementalalterconfigsresponseresponse.ResourceName = &resourcename
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return incrementalalterconfigsresponseresponse, err
		}
		incrementalalterconfigsresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return incrementalalterconfigsresponseresponse, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *IncrementalAlterConfigsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- IncrementalAlterConfigsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Responses != nil {
		fmt.Fprintf(w, "        Responses:\n")
		for _, responses := range *res.Responses {
			fmt.Fprintf(w, "%s", responses.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Responses: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *IncrementalAlterConfigsResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	fmt.Fprintf(w, "            ResourceType: %v\n", value.ResourceType)

	if value.ResourceName != nil {
		fmt.Fprintf(w, "            ResourceName: %v\n", *value.ResourceName)
	} else {
		fmt.Fprintf(w, "            ResourceName: nil\n")
	}

	return w.String()
}
