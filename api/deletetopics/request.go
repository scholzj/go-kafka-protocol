package deletetopics

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteTopicsRequest struct {
	ApiVersion      int16
	Topics          *[]DeleteTopicsRequestTopic // The name or topic ID of the topic. (versions: 6+)
	TopicNames      *[]string                   // The names of the topics to delete. (versions: 0-5)
	TimeoutMs       int32                       // The length of time in milliseconds to wait for the deletions to complete. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteTopicsRequestTopic struct {
	Name            *string   // The topic name. (versions: 6+, nullable: 6+)
	TopicId         uuid.UUID // The unique topic ID. (versions: 6+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (req *DeleteTopicsRequest) Write(w io.Writer) error {
	// Topics (versions: 6+)
	if req.ApiVersion >= 6 {
		if req.Topics == nil {
			return fmt.Errorf("DeleteTopicsRequest.Topics must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.topicsEncoder, *req.Topics); err != nil {
				return err
			}
		}
	}

	// TopicNames (versions: 0-5)
	if req.ApiVersion <= 5 {
		if req.TopicNames == nil {
			return fmt.Errorf("DeleteTopicsRequest.TopicNames must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.TopicNames); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteString, *req.TopicNames); err != nil {
				return err
			}
		}
	}

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
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
func (req *DeleteTopicsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DeleteTopicsRequest.Read: request or its body is nil")
	}

	*req = DeleteTopicsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Topics (versions: 6+)
	if req.ApiVersion >= 6 {
		if isRequestFlexible(req.ApiVersion) {
			topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = &topics
		} else {
			topics, err := protocol.ReadArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = &topics
		}
	}

	// TopicNames (versions: 0-5)
	if req.ApiVersion <= 5 {
		if isRequestFlexible(req.ApiVersion) {
			topicnames, err := protocol.ReadCompactArray(r, protocol.ReadCompactString)
			if err != nil {
				return err
			}
			req.TopicNames = &topicnames
		} else {
			topicnames, err := protocol.ReadArray(r, protocol.ReadString)
			if err != nil {
				return err
			}
			req.TopicNames = &topicnames
		}
	}

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

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

func (req *DeleteTopicsRequest) topicsEncoder(w io.Writer, value DeleteTopicsRequestTopic) error {
	// Name (versions: 6+)
	if req.ApiVersion >= 6 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Name); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Name); err != nil {
				return err
			}
		}
	}

	// TopicId (versions: 6+)
	if req.ApiVersion >= 6 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *DeleteTopicsRequest) topicsDecoder(r io.Reader) (DeleteTopicsRequestTopic, error) {
	deletetopicsrequesttopic := DeleteTopicsRequestTopic{}

	// Name (versions: 6+)
	if req.ApiVersion >= 6 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return deletetopicsrequesttopic, err
			}
			deletetopicsrequesttopic.Name = name
		} else {
			name, err := protocol.ReadNullableString(r)
			if err != nil {
				return deletetopicsrequesttopic, err
			}
			deletetopicsrequesttopic.Name = name
		}
	}

	// TopicId (versions: 6+)
	if req.ApiVersion >= 6 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return deletetopicsrequesttopic, err
		}
		deletetopicsrequesttopic.TopicId = topicid
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deletetopicsrequesttopic, err
		}
		deletetopicsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return deletetopicsrequesttopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DeleteTopicsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DeleteTopicsRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	if req.TopicNames != nil {
		fmt.Fprintf(w, "        TopicNames: %v\n", *req.TopicNames)
	} else {
		fmt.Fprintf(w, "        TopicNames: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DeleteTopicsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)

	return w.String()
}
