package alterpartitionreassignments

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterPartitionReassignmentsRequest struct {
	ApiVersion                   int16
	TimeoutMs                    int32                                      // The time in ms to wait for the request to complete. (versions: 0+)
	AllowReplicationFactorChange bool                                       // The option indicating whether changing the replication factor of any given partition as part of this request is a valid move. (versions: 1+)
	Topics                       *[]AlterPartitionReassignmentsRequestTopic // The topics to reassign. (versions: 0+)
	rawTaggedFields              *[]protocol.TaggedField
}

type AlterPartitionReassignmentsRequestTopic struct {
	Name            *string                                             // The topic name. (versions: 0+)
	Partitions      *[]AlterPartitionReassignmentsRequestTopicPartition // The partitions to reassign. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterPartitionReassignmentsRequestTopicPartition struct {
	PartitionIndex  int32    // The partition index. (versions: 0+)
	Replicas        *[]int32 // The replicas to place the partitions on, or null to cancel a pending reassignment for this partition. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *AlterPartitionReassignmentsRequest) Write(w io.Writer) error {
	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// AllowReplicationFactorChange (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.AllowReplicationFactorChange); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("AlterPartitionReassignmentsRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *AlterPartitionReassignmentsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AlterPartitionReassignmentsRequest.Read: request or its body is nil")
	}

	*req = AlterPartitionReassignmentsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.TimeoutMs = 60000
	req.AllowReplicationFactorChange = true

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// AllowReplicationFactorChange (versions: 1+)
	if req.ApiVersion >= 1 {
		allowreplicationfactorchange, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.AllowReplicationFactorChange = allowreplicationfactorchange
	}

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

func (req *AlterPartitionReassignmentsRequest) topicsEncoder(w io.Writer, value AlterPartitionReassignmentsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AlterPartitionReassignmentsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AlterPartitionReassignmentsRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.partitionsEncoder, *value.Partitions); err != nil {
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

func (req *AlterPartitionReassignmentsRequest) topicsDecoder(r io.Reader) (AlterPartitionReassignmentsRequestTopic, error) {
	alterpartitionreassignmentsrequesttopic := AlterPartitionReassignmentsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterpartitionreassignmentsrequesttopic, err
		}
		alterpartitionreassignmentsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return alterpartitionreassignmentsrequesttopic, err
		}
		alterpartitionreassignmentsrequesttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return alterpartitionreassignmentsrequesttopic, err
		}
		alterpartitionreassignmentsrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return alterpartitionreassignmentsrequesttopic, err
		}
		alterpartitionreassignmentsrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionreassignmentsrequesttopic, err
		}
		alterpartitionreassignmentsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionreassignmentsrequesttopic, nil
}

func (req *AlterPartitionReassignmentsRequest) partitionsEncoder(w io.Writer, value AlterPartitionReassignmentsRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// Replicas (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Replicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, protocol.WriteInt32, value.Replicas); err != nil {
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

func (req *AlterPartitionReassignmentsRequest) partitionsDecoder(r io.Reader) (AlterPartitionReassignmentsRequestTopicPartition, error) {
	alterpartitionreassignmentsrequesttopicpartition := AlterPartitionReassignmentsRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return alterpartitionreassignmentsrequesttopicpartition, err
	}
	alterpartitionreassignmentsrequesttopicpartition.PartitionIndex = partitionindex

	// Replicas (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		replicas, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return alterpartitionreassignmentsrequesttopicpartition, err
		}
		alterpartitionreassignmentsrequesttopicpartition.Replicas = replicas
	} else {
		replicas, err := protocol.ReadNullableArray(r, protocol.ReadInt32)
		if err != nil {
			return alterpartitionreassignmentsrequesttopicpartition, err
		}
		alterpartitionreassignmentsrequesttopicpartition.Replicas = replicas
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterpartitionreassignmentsrequesttopicpartition, err
		}
		alterpartitionreassignmentsrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return alterpartitionreassignmentsrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AlterPartitionReassignmentsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AlterPartitionReassignmentsRequest:\n")
	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)
	fmt.Fprintf(w, "        AllowReplicationFactorChange: %v\n", req.AllowReplicationFactorChange)

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterPartitionReassignmentsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterPartitionReassignmentsRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)

	if value.Replicas != nil {
		fmt.Fprintf(w, "                Replicas: %v\n", *value.Replicas)
	} else {
		fmt.Fprintf(w, "                Replicas: nil\n")
	}

	return w.String()
}
