package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

type MetadataRequest struct {
	ApiVersion                         int16
	Topics                             []MetadataRequestTopic
	AllowAutoTopicCreation             bool
	IncludeClusterAuthorizedOperations bool
	IncludeTopicAuthorizedOperations   bool
	rawTaggedFields                    []protocol.TaggedField
}

type MetadataRequestTopic struct {
	Id              uuid.UUID
	Name            string
	rawTaggedFields []protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (req *MetadataRequest) Write(w io.Writer) error {
	if err := protocol.WriteCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
		return err
	}

	// Allow auto-topic-creation
	if req.ApiVersion >= 4 {
		if err := protocol.WriteBool(w, req.AllowAutoTopicCreation); err != nil {
			return err
		}
	}

	// Include cluster authorized operations
	if req.ApiVersion >= 8 && req.ApiVersion <= 10 {
		if err := protocol.WriteBool(w, req.IncludeClusterAuthorizedOperations); err != nil {
			return err
		}
	}

	// Include topic authorized operations
	if req.ApiVersion >= 8 {
		if err := protocol.WriteBool(w, req.IncludeTopicAuthorizedOperations); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		// Tagged fields
		if err := protocol.WriteRawTaggedFields(w, req.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *MetadataRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Topics
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
	}

	// Allow auto-topic-creation
	if req.ApiVersion >= 4 {
		allowAutoTopicCreation, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.AllowAutoTopicCreation = allowAutoTopicCreation
	}

	// Include cluster authorized operations
	if req.ApiVersion >= 8 && req.ApiVersion <= 10 {
		includeClusterAuthorizedOperations, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeClusterAuthorizedOperations = includeClusterAuthorizedOperations
	}

	// Include topic authorized operations
	if req.ApiVersion >= 8 {
		includeTopicAuthorizedOperations, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeTopicAuthorizedOperations = includeTopicAuthorizedOperations
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		// Tagged fields
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return err
		}
		req.rawTaggedFields = rawTaggedFields
	}

	return nil
}

func (req *MetadataRequest) topicsEncoder(w io.Writer, value MetadataRequestTopic) error {
	// Id
	if req.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.Id); err != nil {
			return err
		}
	}

	// Topic name
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, value.Name); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (req *MetadataRequest) topicsDecoder(r io.Reader) (MetadataRequestTopic, error) {
	topics := MetadataRequestTopic{}

	// Id
	if req.ApiVersion >= 10 {
		id, err := protocol.ReadUUID(r)
		if err != nil {
			return topics, err
		}
		topics.Id = id
	}

	// Topic name
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return topics, err
		}
		topics.Name = name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return topics, err
		}
		topics.Name = name
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return topics, err
		}
		topics.rawTaggedFields = rawTaggedFields
	}

	return topics, nil
}

func (req *MetadataRequest) PrettyPrint() {
	fmt.Printf("-> MetadataRequest:\n")
	fmt.Printf("        Topics:\n")
	for _, topic := range req.Topics {
		fmt.Printf("                Id: %s; Name: %s\n", topic.Id.String(), topic.Name)
	}
	fmt.Printf("        AllowAutoTopicCreation: %t\n", req.AllowAutoTopicCreation)
	fmt.Printf("        IncludeClusterAuthorizedOperations: %t\n", req.IncludeClusterAuthorizedOperations)
	fmt.Printf("        IncludeTopicAuthorizedOperations: %t\n", req.IncludeTopicAuthorizedOperations)
	fmt.Printf("\n")
}
