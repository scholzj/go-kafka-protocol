package metadata

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type MetadataResponse struct {
	ApiVersion                  int16
	ThrottleTimeMs              int32                     // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	Brokers                     *[]MetadataResponseBroker // A list of brokers present in the cluster.
	ClusterId                   *string                   // The cluster ID that responding broker belongs to.
	ControllerId                int32                     // The ID of the controller broker.
	Topics                      *[]MetadataResponseTopic  // Each topic in the response.
	ClusterAuthorizedOperations int32                     // 32-bit bitfield to represent authorized operations for this cluster.
	ErrorCode                   int16                     // The top-level error code, or 0 if there was no error.
	rawTaggedFields             *[]protocol.TaggedField
}

type MetadataResponseBroker struct {
	NodeId          int32   // The broker ID.
	Host            *string // The broker hostname.
	Port            int32   // The broker port.
	Rack            *string // The rack of the broker, or null if it has not been assigned to a rack.
	rawTaggedFields *[]protocol.TaggedField
}

type MetadataResponseTopic struct {
	ErrorCode                 int16                             // The topic error, or 0 if there was no error.
	Name                      *string                           // The topic name. Null for non-existing topics queried by ID. This is never null when ErrorCode is zero. One of Name and TopicId is always populated.
	TopicId                   uuid.UUID                         // The topic id. Zero for non-existing topics queried by name. This is never zero when ErrorCode is zero. One of Name and TopicId is always populated.
	IsInternal                bool                              // True if the topic is internal.
	Partitions                *[]MetadataResponseTopicPartition // Each partition in the topic.
	TopicAuthorizedOperations int32                             // 32-bit bitfield to represent authorized operations for this topic.
	rawTaggedFields           *[]protocol.TaggedField
}

type MetadataResponseTopicPartition struct {
	ErrorCode       int16    // The partition error, or 0 if there was no error.
	PartitionIndex  int32    // The partition index.
	LeaderId        int32    // The ID of the leader broker.
	LeaderEpoch     int32    // The leader epoch of this partition.
	ReplicaNodes    *[]int32 // The set of all nodes that host this partition.
	IsrNodes        *[]int32 // The set of nodes that are in sync with the leader for this partition.
	OfflineReplicas *[]int32 // The set of offline replicas of this partition.
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (res *MetadataResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Brokers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.brokersEncoder, res.Brokers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.brokersEncoder, *res.Brokers); err != nil {
			return err
		}
	}

	// ClusterId (versions: 2+)
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *res.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.ClusterId); err != nil {
				return err
			}
		}
	}

	// ControllerId (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ControllerId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
			return err
		}
	}

	// ClusterAuthorizedOperations (versions: 8-10)
	if res.ApiVersion >= 8 && res.ApiVersion <= 10 {
		if err := protocol.WriteInt32(w, res.ClusterAuthorizedOperations); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 13+)
	if res.ApiVersion >= 13 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
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
func (res *MetadataResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// ThrottleTimeMs (versions: 3+)
	if response.ApiVersion >= 3 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// Brokers (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		brokers, err := protocol.ReadNullableCompactArray(r, res.brokersDecoder)
		if err != nil {
			return err
		}
		res.Brokers = brokers
	} else {
		brokers, err := protocol.ReadArray(r, res.brokersDecoder)
		if err != nil {
			return err
		}
		res.Brokers = &brokers
	}

	// ClusterId (versions: 2+)
	if response.ApiVersion >= 2 {
		if isRequestFlexible(res.ApiVersion) {
			clusterid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			res.ClusterId = &clusterid
		} else {
			clusterid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.ClusterId = &clusterid
		}
	}

	// ControllerId (versions: 1+)
	if response.ApiVersion >= 1 {
		controllerid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ControllerId = controllerid
	}

	// Topics (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	}

	// ClusterAuthorizedOperations (versions: 8-10)
	if response.ApiVersion >= 8 && response.ApiVersion <= 10 {
		clusterauthorizedoperations, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ClusterAuthorizedOperations = clusterauthorizedoperations
	}

	// ErrorCode (versions: 13+)
	if response.ApiVersion >= 13 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *MetadataResponse) brokersEncoder(w io.Writer, value MetadataResponseBroker) error {
	// NodeId (versions: 0+)
	if err := protocol.WriteInt32(w, value.NodeId); err != nil {
		return err
	}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port (versions: 0+)
	if err := protocol.WriteInt32(w, value.Port); err != nil {
		return err
	}

	// Rack (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.Rack); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.Rack); err != nil {
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

func (res *MetadataResponse) brokersDecoder(r io.Reader) (MetadataResponseBroker, error) {
	metadataresponsebroker := MetadataResponseBroker{}
	var err error

	// NodeId (versions: 0+)
	nodeid, err := protocol.ReadInt32(r)
	if err != nil {
		return metadataresponsebroker, err
	}
	metadataresponsebroker.NodeId = nodeid

	// Host (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return metadataresponsebroker, err
		}
		metadataresponsebroker.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return metadataresponsebroker, err
		}
		metadataresponsebroker.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return metadataresponsebroker, err
	}
	metadataresponsebroker.Port = port

	// Rack (versions: 1+)
	if res.ApiVersion >= 1 {
		if isRequestFlexible(res.ApiVersion) {
			rack, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return metadataresponsebroker, err
			}
			metadataresponsebroker.Rack = rack
		} else {
			rack, err := protocol.ReadNullableString(r)
			if err != nil {
				return metadataresponsebroker, err
			}
			metadataresponsebroker.Rack = rack
		}
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return metadataresponsebroker, err
		}
		metadataresponsebroker.rawTaggedFields = &rawTaggedFields
	}

	return metadataresponsebroker, nil
}

func (res *MetadataResponse) topicsEncoder(w io.Writer, value MetadataResponseTopic) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Name); err != nil {
			return err
		}
	}

	// TopicId (versions: 10+)
	if res.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// IsInternal (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, value.IsInternal); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
			return err
		}
	}

	// TopicAuthorizedOperations (versions: 8+)
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt32(w, value.TopicAuthorizedOperations); err != nil {
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

func (res *MetadataResponse) topicsDecoder(r io.Reader) (MetadataResponseTopic, error) {
	metadataresponsetopic := MetadataResponseTopic{}
	var err error

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return metadataresponsetopic, err
	}
	metadataresponsetopic.ErrorCode = errorcode

	// Name (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		name, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.Name = name
	} else {
		name, err := protocol.ReadNullableString(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.Name = name
	}

	// TopicId (versions: 10+)
	if res.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.TopicId = topicid
	}

	// IsInternal (versions: 1+)
	if res.ApiVersion >= 1 {
		isinternal, err := protocol.ReadBool(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.IsInternal = isinternal
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.Partitions = &partitions
	}

	// TopicAuthorizedOperations (versions: 8+)
	if res.ApiVersion >= 8 {
		topicauthorizedoperations, err := protocol.ReadInt32(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.TopicAuthorizedOperations = topicauthorizedoperations
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return metadataresponsetopic, err
		}
		metadataresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return metadataresponsetopic, nil
}

func (res *MetadataResponse) partitionsEncoder(w io.Writer, value MetadataResponseTopicPartition) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 7+)
	if res.ApiVersion >= 7 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
			return err
		}
	}

	// ReplicaNodes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.ReplicaNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.ReplicaNodes); err != nil {
			return err
		}
	}

	// IsrNodes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.IsrNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.IsrNodes); err != nil {
			return err
		}
	}

	// OfflineReplicas (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.OfflineReplicas); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.OfflineReplicas); err != nil {
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

func (res *MetadataResponse) partitionsDecoder(r io.Reader) (MetadataResponseTopicPartition, error) {
	metadataresponsetopicpartition := MetadataResponseTopicPartition{}
	var err error

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return metadataresponsetopicpartition, err
	}
	metadataresponsetopicpartition.ErrorCode = errorcode

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return metadataresponsetopicpartition, err
	}
	metadataresponsetopicpartition.PartitionIndex = partitionindex

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return metadataresponsetopicpartition, err
	}
	metadataresponsetopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 7+)
	if res.ApiVersion >= 7 {
		leaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.LeaderEpoch = leaderepoch
	}

	// ReplicaNodes (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		replicanodes, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.ReplicaNodes = replicanodes
	} else {
		replicanodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.ReplicaNodes = &replicanodes
	}

	// IsrNodes (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		isrnodes, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.IsrNodes = isrnodes
	} else {
		isrnodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.IsrNodes = &isrnodes
	}

	// OfflineReplicas (versions: 5+)
	if res.ApiVersion >= 5 {
		if isRequestFlexible(res.ApiVersion) {
			offlinereplicas, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return metadataresponsetopicpartition, err
			}
			metadataresponsetopicpartition.OfflineReplicas = offlinereplicas
		} else {
			offlinereplicas, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return metadataresponsetopicpartition, err
			}
			metadataresponsetopicpartition.OfflineReplicas = &offlinereplicas
		}
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return metadataresponsetopicpartition, err
		}
		metadataresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return metadataresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *MetadataResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- MetadataResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	if res.Brokers != nil {
		fmt.Fprintf(w, "        Brokers:\n")
		for _, brokers := range *res.Brokers {
			fmt.Fprintf(w, "%s", brokers.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Brokers: nil\n")
	}
	if res.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *res.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}
	fmt.Fprintf(w, "        ControllerId: %v\n", res.ControllerId)
	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}
	fmt.Fprintf(w, "        ClusterAuthorizedOperations: %v\n", res.ClusterAuthorizedOperations)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *MetadataResponseBroker) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)
	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}
	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	if value.Rack != nil {
		fmt.Fprintf(w, "            Rack: %v\n", *value.Rack)
	} else {
		fmt.Fprintf(w, "            Rack: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *MetadataResponseTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)
	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}
	fmt.Fprintf(w, "            TopicId: %v\n", value.TopicId)
	fmt.Fprintf(w, "            IsInternal: %v\n", value.IsInternal)
	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}
	fmt.Fprintf(w, "            TopicAuthorizedOperations: %v\n", value.TopicAuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *MetadataResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)
	if value.ReplicaNodes != nil {
		fmt.Fprintf(w, "                ReplicaNodes: %v\n", *value.ReplicaNodes)
	} else {
		fmt.Fprintf(w, "                ReplicaNodes: nil\n")
	}
	if value.IsrNodes != nil {
		fmt.Fprintf(w, "                IsrNodes: %v\n", *value.IsrNodes)
	} else {
		fmt.Fprintf(w, "                IsrNodes: nil\n")
	}
	if value.OfflineReplicas != nil {
		fmt.Fprintf(w, "                OfflineReplicas: %v\n", *value.OfflineReplicas)
	} else {
		fmt.Fprintf(w, "                OfflineReplicas: nil\n")
	}

	return w.String()
}
