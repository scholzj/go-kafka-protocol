package metadata

import (
	"bytes"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

type MetadataResponse struct {
	ApiVersion                  int16
	ThrottleTimeMs              int32
	Brokers                     []MetadataResponseBroker
	ClusterId                   *string
	ControllerId                int32
	Topics                      []MetadataResponseTopic
	ClusterAuthorizedOperations int32
	ErrorCode                   int16
	rawTaggedFields             []protocol.TaggedField
}

type MetadataResponseBroker struct {
	NodeId          int32
	Host            string
	Port            int32
	Rack            *string
	rawTaggedFields []protocol.TaggedField
}

type MetadataResponseTopic struct {
	ErrorCode                 int16
	Name                      *string
	Id                        uuid.UUID
	IsInternal                bool
	Partitions                []MetadataResponseTopicPartition
	TopicAuthorizedOperations int32
	rawTaggedFields           []protocol.TaggedField
}

type MetadataResponseTopicPartition struct {
	ErrorCode       int16
	PartitionIndex  int32
	LeaderId        int32
	LeaderEpoch     int32
	ReplicaNodes    []int32
	IsrNodes        []int32
	OfflineReplicas []int32
	rawTaggedFields []protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 9
}

func (res *MetadataResponse) Write(w io.Writer) error {
	// ThrottleTime
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Brokers
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactArray(w, res.brokerEncoder, res.Brokers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.brokerEncoder, res.Brokers); err != nil {
			return err
		}
	}

	// Cluster Id
	if res.ApiVersion >= 2 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactNullableString(w, res.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ClusterId); err != nil {
				return err
			}
		}
	}

	// Controller Id
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ControllerId); err != nil {
			return err
		}
	}

	// Topics
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactArray(w, res.topicEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicEncoder, res.Topics); err != nil {
			return err
		}
	}

	// Cluster Authorized Operations
	if res.ApiVersion >= 8 && res.ApiVersion <= 10 {
		if err := protocol.WriteInt32(w, res.ClusterAuthorizedOperations); err != nil {
			return err
		}
	}

	// ErrorCode
	if res.ApiVersion >= 13 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, res.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *MetadataResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTime
	if res.ApiVersion >= 3 {
		throttleTimeMs, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttleTimeMs
	}

	// Brokers
	if isRequestFlexible(res.ApiVersion) {
		brokers, err := protocol.ReadCompactArray(r, res.brokerDecoder)
		if err != nil {
			return err
		}
		res.Brokers = brokers
	} else {
		brokers, err := protocol.ReadArray(r, res.brokerDecoder)
		if err != nil {
			return err
		}
		res.Brokers = brokers
	}

	// Cluster ID
	if res.ApiVersion >= 2 {
		if isRequestFlexible(res.ApiVersion) {
			clusterId, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			res.ClusterId = clusterId
		} else {
			clusterId, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			res.ClusterId = clusterId
		}
	}

	// Controller ID
	if res.ApiVersion >= 1 {
		controllerId, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ControllerId = controllerId
	} else {
		res.ControllerId = -1
	}

	// Topics
	if isRequestFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicDecoder)
		if err != nil {
			return err
		}
		res.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicDecoder)
		if err != nil {
			return err
		}
		res.Topics = topics
	}

	// Cluster Authorized Operations
	if res.ApiVersion >= 8 && res.ApiVersion <= 10 {
		clusterAuthorizedOperations, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ClusterAuthorizedOperations = clusterAuthorizedOperations
	} else {
		res.ClusterAuthorizedOperations = -2147483648
	}

	// ErrorCode
	if res.ApiVersion >= 13 {
		errorCode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorCode
	}

	// Tagged fields
	if isResponseFlexible(response.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return err
		}
		res.rawTaggedFields = rawTaggedFields
	}

	return nil
}

func (res *MetadataResponse) brokerEncoder(w io.Writer, value MetadataResponseBroker) error {
	// Node Id
	if err := protocol.WriteInt32(w, value.NodeId); err != nil {
		return err
	}

	// Host
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, value.Host); err != nil {
			return err
		}
	}

	// Port
	if err := protocol.WriteInt32(w, value.Port); err != nil {
		return err
	}

	// Is Internal
	if res.ApiVersion >= 1 {
		// Host
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactNullableString(w, value.Rack); err != nil {
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
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *MetadataResponse) brokerDecoder(r io.Reader) (MetadataResponseBroker, error) {
	broker := MetadataResponseBroker{}

	// NodeId
	nodeId, err := protocol.ReadInt32(r)
	if err != nil {
		return broker, err
	}
	broker.NodeId = nodeId

	// Name
	if isRequestFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return broker, err
		}
		broker.Host = host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return broker, err
		}
		broker.Host = host
	}

	// Port
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return broker, err
	}
	broker.Port = port

	// Rack
	if res.ApiVersion >= 1 {
		if isRequestFlexible(res.ApiVersion) {
			rack, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return broker, err
			}
			broker.Rack = rack
		} else {
			rack, err := protocol.ReadNullableString(r)
			if err != nil {
				return broker, err
			}
			broker.Rack = rack
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return broker, err
		}
		broker.rawTaggedFields = rawTaggedFields
	}

	return broker, nil
}

func (res *MetadataResponse) topicEncoder(w io.Writer, value MetadataResponseTopic) error {
	// Error Code
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// Name
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactNullableString(w, value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Name); err != nil {
			return err
		}
	}

	// Topic ID
	if res.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.Id); err != nil {
			return err
		}
	}

	// Is Internal
	if res.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, value.IsInternal); err != nil {
			return err
		}
	}

	// Partitions
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactArray(w, res.topicPartitionEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicPartitionEncoder, value.Partitions); err != nil {
			return err
		}
	}

	// Topic Authorized Operations
	if res.ApiVersion >= 8 {
		if err := protocol.WriteInt32(w, value.TopicAuthorizedOperations); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *MetadataResponse) topicDecoder(r io.Reader) (MetadataResponseTopic, error) {
	topicPart := MetadataResponseTopic{
		TopicAuthorizedOperations: -2147483648,
	}

	// Error Code
	errorCode, err := protocol.ReadInt16(r)
	if err != nil {
		return topicPart, err
	}
	topicPart.ErrorCode = errorCode

	// Name
	if isRequestFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactNullableString(r)
		if err != nil {
			return topicPart, err
		}
		topicPart.Name = name
	} else {
		name, err := protocol.ReadNullableString(r)
		if err != nil {
			return topicPart, err
		}
		topicPart.Name = name
	}

	// Id
	if res.ApiVersion >= 10 {
		id, err := protocol.ReadUUID(r)
		if err != nil {
			return topicPart, err
		}
		topicPart.Id = id
	}

	// IsInternal
	if res.ApiVersion >= 1 {
		isInternal, err := protocol.ReadBool(r)
		if err != nil {
			return topicPart, err
		}
		topicPart.IsInternal = isInternal
	}

	// Partitions
	if isRequestFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.topicPartitionDecoder)
		if err != nil {
			return topicPart, err
		}
		topicPart.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.topicPartitionDecoder)
		if err != nil {
			return topicPart, err
		}
		topicPart.Partitions = partitions
	}

	// Topic Authorized Operations
	if res.ApiVersion >= 8 {
		topicAuthorizedOperations, err := protocol.ReadInt32(r)
		if err != nil {
			return topicPart, err
		}
		topicPart.TopicAuthorizedOperations = topicAuthorizedOperations
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return topicPart, err
		}
		topicPart.rawTaggedFields = rawTaggedFields
	}

	return topicPart, nil
}

func (res *MetadataResponse) topicPartitionEncoder(w io.Writer, value MetadataResponseTopicPartition) error {
	// Error Code
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// PArtition Index
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// LeaderId
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// Leader Epoch
	if res.ApiVersion >= 7 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
			return err
		}
	}

	// ReplicaNodes
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactArray(w, protocol.WriteInt32, value.ReplicaNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, value.ReplicaNodes); err != nil {
			return err
		}
	}

	// ISR Nodes
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactArray(w, protocol.WriteInt32, value.IsrNodes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, value.IsrNodes); err != nil {
			return err
		}
	}

	// Offline Replicas
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactArray(w, protocol.WriteInt32, value.OfflineReplicas); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, value.OfflineReplicas); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, value.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *MetadataResponse) topicPartitionDecoder(r io.Reader) (MetadataResponseTopicPartition, error) {
	topicPart := MetadataResponseTopicPartition{
		LeaderEpoch: -1,
	}

	// Error Code
	errorCode, err := protocol.ReadInt16(r)
	if err != nil {
		return topicPart, err
	}
	topicPart.ErrorCode = errorCode

	// Partition Index
	partitionIndex, err := protocol.ReadInt32(r)
	if err != nil {
		return topicPart, err
	}
	topicPart.PartitionIndex = partitionIndex

	// Leader ID
	leaderId, err := protocol.ReadInt32(r)
	if err != nil {
		return topicPart, err
	}
	topicPart.LeaderId = leaderId

	// Replica Nodes
	if isRequestFlexible(res.ApiVersion) {
		replicaNodes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return topicPart, err
		}
		topicPart.ReplicaNodes = replicaNodes
	} else {
		replicaNodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return topicPart, err
		}
		topicPart.ReplicaNodes = replicaNodes
	}

	// ISR Nodes
	if isRequestFlexible(res.ApiVersion) {
		isrNodes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return topicPart, err
		}
		topicPart.IsrNodes = isrNodes
	} else {
		isrNodes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return topicPart, err
		}
		topicPart.IsrNodes = isrNodes
	}

	// OfflineReplicas
	if res.ApiVersion >= 5 {
		if isRequestFlexible(res.ApiVersion) {
			offlineReplicas, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return topicPart, err
			}
			topicPart.OfflineReplicas = offlineReplicas
		} else {
			offlineReplicas, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return topicPart, err
			}
			topicPart.OfflineReplicas = offlineReplicas
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return topicPart, err
		}
		topicPart.rawTaggedFields = rawTaggedFields
	}

	return topicPart, nil
}

func (res *MetadataResponse) PrettyPrint() {
	fmt.Printf("<- MetadataResponse:\n")
	fmt.Printf("        ThrottleTimeMs: %d\n", res.ThrottleTimeMs)
	fmt.Printf("        Brokers:\n")
	for _, broker := range res.Brokers {
		fmt.Printf("                Id: %d\n", broker.NodeId)
		fmt.Printf("                Host: %s\n", broker.Host)
		fmt.Printf("                Port: %d\n", broker.Port)
		fmt.Printf("                Rack: %s\n", *broker.Rack)
		fmt.Printf("                ----------\n")
	}
	fmt.Printf("        ClusterId: %s\n", *res.ClusterId)
	fmt.Printf("        ControllerId: %d\n", res.ControllerId)
	fmt.Printf("        Topics:\n")
	for _, topic := range res.Topics {
		fmt.Printf("                ErrorCode: %d\n", topic.ErrorCode)
		fmt.Printf("                Name: %s\n", *topic.Name)
		fmt.Printf("                Id: %s\n", topic.Id.String())
		fmt.Printf("                IsInternal: %t\n", topic.IsInternal)
		fmt.Printf("                Partitions:\n")
		for _, partition := range topic.Partitions {
			fmt.Printf("                        ErrorCode: %d\n", partition.ErrorCode)
			fmt.Printf("                        Index: %d\n", partition.PartitionIndex)
			fmt.Printf("                        LeaderId: %d\n", partition.LeaderId)
			fmt.Printf("                        LeaderEpoch: %d\n", partition.LeaderEpoch)
			fmt.Printf("                        ReplicaNodes: %v\n", partition.ReplicaNodes)
			fmt.Printf("                        IsrNodes: %v\n", partition.IsrNodes)
			fmt.Printf("                        OfflineReplicas: %v\n", partition.OfflineReplicas)
			fmt.Printf("                        ----------\n")
		}
		fmt.Printf("                TopicAuthorizedOperations: %d\n", topic.TopicAuthorizedOperations)
		fmt.Printf("                ----------\n")
	}
	fmt.Printf("        TopicAuthorizedOperations: %d\n", res.ClusterAuthorizedOperations)
	fmt.Printf("        ErrorCode: %d\n", res.ErrorCode)
	fmt.Printf("\n")
}
