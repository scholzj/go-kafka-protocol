package deletetopics

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteTopicsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                           // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 1+)
	Responses       *[]DeleteTopicsResponseResponse // The results for each topic we tried to delete. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteTopicsResponseResponse struct {
	Name            *string   // The topic name. (versions: 0+, nullable: 6+)
	TopicId         uuid.UUID // The unique topic ID. (versions: 6+)
	ErrorCode       int16     // The deletion error, or 0 if the deletion succeeded. (versions: 0+)
	ErrorMessage    *string   // The error message, or null if there was no error. (versions: 5+, nullable: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (res *DeleteTopicsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Responses (versions: 0+)
	if res.Responses == nil {
		return fmt.Errorf("DeleteTopicsResponse.Responses must not be nil in version %d", res.ApiVersion)
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
func (res *DeleteTopicsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DeleteTopicsResponse.Read: response or its body is nil")
	}

	*res = DeleteTopicsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// Responses (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		responses, err := protocol.ReadCompactArray(r, res.responsesDecoder)
		if err != nil {
			return err
		}
		res.Responses = &responses
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

func (res *DeleteTopicsResponse) responsesEncoder(w io.Writer, value DeleteTopicsResponseResponse) error {
	// Name (versions: 0+)
	if res.ApiVersion < 6 && value.Name == nil {
		return fmt.Errorf("DeleteTopicsResponseResponse.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Name); err != nil {
			return err
		}
	}

	// TopicId (versions: 6+)
	if res.ApiVersion >= 6 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
				return err
			}
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

func (res *DeleteTopicsResponse) responsesDecoder(r io.Reader) (DeleteTopicsResponseResponse, error) {
	deletetopicsresponseresponse := DeleteTopicsResponseResponse{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if res.ApiVersion >= 6 {
			name, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.Name = name
		} else {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.Name = &name
		}
	} else {
		if res.ApiVersion >= 6 {
			name, err := protocol.ReadNullableString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.Name = name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.Name = &name
		}
	}

	// TopicId (versions: 6+)
	if res.ApiVersion >= 6 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return deletetopicsresponseresponse, err
		}
		deletetopicsresponseresponse.TopicId = topicid
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return deletetopicsresponseresponse, err
	}
	deletetopicsresponseresponse.ErrorCode = errorcode

	// ErrorMessage (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			errormessage, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.ErrorMessage = errormessage
		} else {
			errormessage, err := protocol.ReadNullableString(r)
			if err != nil {
				return deletetopicsresponseresponse, err
			}
			deletetopicsresponseresponse.ErrorMessage = errormessage
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deletetopicsresponseresponse, err
		}
		deletetopicsresponseresponse.rawTaggedFields = &rawTaggedFields
	}

	return deletetopicsresponseresponse, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DeleteTopicsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DeleteTopicsResponse:\n")
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
func (value *DeleteTopicsResponseResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	return w.String()
}
