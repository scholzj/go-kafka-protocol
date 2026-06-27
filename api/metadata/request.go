package metadata

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type MetadataRequest struct {
	ApiVersion                         int16
	Topics                             *[]MetadataRequestTopic // The topics to fetch metadata for. (versions: 0+, nullable: 1+)
	AllowAutoTopicCreation             bool                    // If this is true, the broker may auto-create topics that we requested which do not already exist, if it is configured to do so. (versions: 4+)
	IncludeClusterAuthorizedOperations bool                    // Whether to include cluster authorized operations. (versions: 8-10)
	IncludeTopicAuthorizedOperations   bool                    // Whether to include topic authorized operations. (versions: 8+)
	rawTaggedFields                    *[]protocol.TaggedField
}

type MetadataRequestTopic struct {
	TopicId         uuid.UUID // The topic id. (versions: 10+)
	Name            *string   // The topic name. (versions: 0+, nullable: 10+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (req *MetadataRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.ApiVersion < 1 && req.Topics == nil {
		return fmt.Errorf("MetadataRequest.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	}

	// AllowAutoTopicCreation (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteBool(w, req.AllowAutoTopicCreation); err != nil {
			return err
		}
	}

	// IncludeClusterAuthorizedOperations (versions: 8-10)
	if req.ApiVersion >= 8 && req.ApiVersion <= 10 {
		if err := protocol.WriteBool(w, req.IncludeClusterAuthorizedOperations); err != nil {
			return err
		}
	}

	// IncludeTopicAuthorizedOperations (versions: 8+)
	if req.ApiVersion >= 8 {
		if err := protocol.WriteBool(w, req.IncludeTopicAuthorizedOperations); err != nil {
			return err
		}
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
func (req *MetadataRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("MetadataRequest.Read: request or its body is nil")
	}

	*req = MetadataRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.AllowAutoTopicCreation = true

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 1 {
			topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = topics
		} else {
			topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = &topics
		}
	} else {
		if req.ApiVersion >= 1 {
			topics, err := protocol.ReadNullableArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = topics
		} else {
			topics, err := protocol.ReadArray(r, req.topicsDecoder)
			if err != nil {
				return err
			}
			req.Topics = &topics
		}
	}

	// AllowAutoTopicCreation (versions: 4+)
	if req.ApiVersion >= 4 {
		allowautotopiccreation, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.AllowAutoTopicCreation = allowautotopiccreation
	}

	// IncludeClusterAuthorizedOperations (versions: 8-10)
	if req.ApiVersion >= 8 && req.ApiVersion <= 10 {
		includeclusterauthorizedoperations, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeClusterAuthorizedOperations = includeclusterauthorizedoperations
	}

	// IncludeTopicAuthorizedOperations (versions: 8+)
	if req.ApiVersion >= 8 {
		includetopicauthorizedoperations, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeTopicAuthorizedOperations = includetopicauthorizedoperations
	}

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

func (req *MetadataRequest) topicsEncoder(w io.Writer, value MetadataRequestTopic) error {
	// TopicId (versions: 10+)
	if req.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// Name (versions: 0+)
	if req.ApiVersion < 10 && value.Name == nil {
		return fmt.Errorf("MetadataRequestTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Name); err != nil {
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

func (req *MetadataRequest) topicsDecoder(r io.Reader) (MetadataRequestTopic, error) {
	metadatarequesttopic := MetadataRequestTopic{}

	// TopicId (versions: 10+)
	if req.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return metadatarequesttopic, err
		}
		metadatarequesttopic.TopicId = topicid
	}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 10 {
			name, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return metadatarequesttopic, err
			}
			metadatarequesttopic.Name = name
		} else {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return metadatarequesttopic, err
			}
			metadatarequesttopic.Name = &name
		}
	} else {
		if req.ApiVersion >= 10 {
			name, err := protocol.ReadNullableString(r)
			if err != nil {
				return metadatarequesttopic, err
			}
			metadatarequesttopic.Name = name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return metadatarequesttopic, err
			}
			metadatarequesttopic.Name = &name
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return metadatarequesttopic, err
		}
		metadatarequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return metadatarequesttopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *MetadataRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> MetadataRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        AllowAutoTopicCreation: %v\n", req.AllowAutoTopicCreation)
	fmt.Fprintf(w, "        IncludeClusterAuthorizedOperations: %v\n", req.IncludeClusterAuthorizedOperations)
	fmt.Fprintf(w, "        IncludeTopicAuthorizedOperations: %v\n", req.IncludeTopicAuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *MetadataRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	return w.String()
}
