package createtopics

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreateTopicsRequest struct {
	ApiVersion      int16
	Topics          *[]CreateTopicsRequestTopic // The topics to create. (versions: 0+)
	TimeoutMs       int32                       // How long to wait in milliseconds before timing out the request. (versions: 0+)
	ValidateOnly    bool                        // If true, check that the topics can be created as specified, but don't create anything. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreateTopicsRequestTopic struct {
	Name              *string                               // The topic name. (versions: 0+)
	NumPartitions     int32                                 // The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions. (versions: 0+)
	ReplicationFactor int16                                 // The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor. (versions: 0+)
	Assignments       *[]CreateTopicsRequestTopicAssignment // The manual partition assignment, or the empty array if we are using automatic assignment. (versions: 0+)
	Configs           *[]CreateTopicsRequestTopicConfig     // The custom topic configurations to set. (versions: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

type CreateTopicsRequestTopicAssignment struct {
	PartitionIndex  int32    // The partition index. (versions: 0+)
	BrokerIds       *[]int32 // The brokers to place the partition on. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type CreateTopicsRequestTopicConfig struct {
	Name            *string // The configuration name. (versions: 0+)
	Value           *string // The configuration value. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 5
}

func (req *CreateTopicsRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("CreateTopicsRequest.Topics must not be nil in version %d", req.ApiVersion)
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

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// ValidateOnly (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.ValidateOnly); err != nil {
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
func (req *CreateTopicsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("CreateTopicsRequest.Read: request or its body is nil")
	}

	*req = CreateTopicsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.TimeoutMs = 60000

	// Topics (versions: 0+)
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

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// ValidateOnly (versions: 1+)
	if req.ApiVersion >= 1 {
		validateonly, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.ValidateOnly = validateonly
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

func (req *CreateTopicsRequest) topicsEncoder(w io.Writer, value CreateTopicsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("CreateTopicsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// NumPartitions (versions: 0+)
	if err := protocol.WriteInt32(w, value.NumPartitions); err != nil {
		return err
	}

	// ReplicationFactor (versions: 0+)
	if err := protocol.WriteInt16(w, value.ReplicationFactor); err != nil {
		return err
	}

	// Assignments (versions: 0+)
	if value.Assignments == nil {
		return fmt.Errorf("CreateTopicsRequestTopic.Assignments must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.assignmentsEncoder, value.Assignments); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.assignmentsEncoder, *value.Assignments); err != nil {
			return err
		}
	}

	// Configs (versions: 0+)
	if value.Configs == nil {
		return fmt.Errorf("CreateTopicsRequestTopic.Configs must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.configsEncoder, value.Configs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.configsEncoder, *value.Configs); err != nil {
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

func (req *CreateTopicsRequest) topicsDecoder(r io.Reader) (CreateTopicsRequestTopic, error) {
	createtopicsrequesttopic := CreateTopicsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Name = &name
	}

	// NumPartitions (versions: 0+)
	numpartitions, err := protocol.ReadInt32(r)
	if err != nil {
		return createtopicsrequesttopic, err
	}
	createtopicsrequesttopic.NumPartitions = numpartitions

	// ReplicationFactor (versions: 0+)
	replicationfactor, err := protocol.ReadInt16(r)
	if err != nil {
		return createtopicsrequesttopic, err
	}
	createtopicsrequesttopic.ReplicationFactor = replicationfactor

	// Assignments (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		assignments, err := protocol.ReadCompactArray(r, req.assignmentsDecoder)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Assignments = &assignments
	} else {
		assignments, err := protocol.ReadArray(r, req.assignmentsDecoder)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Assignments = &assignments
	}

	// Configs (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		configs, err := protocol.ReadCompactArray(r, req.configsDecoder)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Configs = &configs
	} else {
		configs, err := protocol.ReadArray(r, req.configsDecoder)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.Configs = &configs
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createtopicsrequesttopic, err
		}
		createtopicsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return createtopicsrequesttopic, nil
}

func (req *CreateTopicsRequest) assignmentsEncoder(w io.Writer, value CreateTopicsRequestTopicAssignment) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// BrokerIds (versions: 0+)
	if value.BrokerIds == nil {
		return fmt.Errorf("CreateTopicsRequestTopicAssignment.BrokerIds must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.BrokerIds); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.BrokerIds); err != nil {
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

func (req *CreateTopicsRequest) assignmentsDecoder(r io.Reader) (CreateTopicsRequestTopicAssignment, error) {
	createtopicsrequesttopicassignment := CreateTopicsRequestTopicAssignment{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return createtopicsrequesttopicassignment, err
	}
	createtopicsrequesttopicassignment.PartitionIndex = partitionindex

	// BrokerIds (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		brokerids, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return createtopicsrequesttopicassignment, err
		}
		createtopicsrequesttopicassignment.BrokerIds = &brokerids
	} else {
		brokerids, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return createtopicsrequesttopicassignment, err
		}
		createtopicsrequesttopicassignment.BrokerIds = &brokerids
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createtopicsrequesttopicassignment, err
		}
		createtopicsrequesttopicassignment.rawTaggedFields = &rawTaggedFields
	}

	return createtopicsrequesttopicassignment, nil
}

func (req *CreateTopicsRequest) configsEncoder(w io.Writer, value CreateTopicsRequestTopicConfig) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("CreateTopicsRequestTopicConfig.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Value); err != nil {
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

func (req *CreateTopicsRequest) configsDecoder(r io.Reader) (CreateTopicsRequestTopicConfig, error) {
	createtopicsrequesttopicconfig := CreateTopicsRequestTopicConfig{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return createtopicsrequesttopicconfig, err
		}
		createtopicsrequesttopicconfig.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return createtopicsrequesttopicconfig, err
		}
		createtopicsrequesttopicconfig.Name = &name
	}

	// Value (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		value, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return createtopicsrequesttopicconfig, err
		}
		createtopicsrequesttopicconfig.Value = value
	} else {
		value, err := protocol.ReadNullableString(r)
		if err != nil {
			return createtopicsrequesttopicconfig, err
		}
		createtopicsrequesttopicconfig.Value = value
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return createtopicsrequesttopicconfig, err
		}
		createtopicsrequesttopicconfig.rawTaggedFields = &rawTaggedFields
	}

	return createtopicsrequesttopicconfig, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *CreateTopicsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> CreateTopicsRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)
	fmt.Fprintf(w, "        ValidateOnly: %v\n", req.ValidateOnly)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateTopicsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            NumPartitions: %v\n", value.NumPartitions)
	fmt.Fprintf(w, "            ReplicationFactor: %v\n", value.ReplicationFactor)

	if value.Assignments != nil {
		fmt.Fprintf(w, "            Assignments:\n")
		for _, assignments := range *value.Assignments {
			fmt.Fprintf(w, "%s", assignments.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Assignments: nil\n")
	}

	if value.Configs != nil {
		fmt.Fprintf(w, "            Configs:\n")
		for _, configs := range *value.Configs {
			fmt.Fprintf(w, "%s", configs.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Configs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateTopicsRequestTopicAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)

	if value.BrokerIds != nil {
		fmt.Fprintf(w, "                BrokerIds: %v\n", *value.BrokerIds)
	} else {
		fmt.Fprintf(w, "                BrokerIds: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *CreateTopicsRequestTopicConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                Value: nil\n")
	}

	return w.String()
}
