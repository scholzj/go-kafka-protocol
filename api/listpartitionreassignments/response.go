package listpartitionreassignments

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListPartitionReassignmentsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                      // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                      // The top-level error code, or 0 if there was no error. (versions: 0+)
	ErrorMessage    *string                                    // The top-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	Topics          *[]ListPartitionReassignmentsResponseTopic // The ongoing reassignments for each topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListPartitionReassignmentsResponseTopic struct {
	Name            *string                                             // The topic name. (versions: 0+)
	Partitions      *[]ListPartitionReassignmentsResponseTopicPartition // The ongoing reassignments for each partition. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListPartitionReassignmentsResponseTopicPartition struct {
	PartitionIndex   int32    // The index of the partition. (versions: 0+)
	Replicas         *[]int32 // The current replica set. (versions: 0+)
	AddingReplicas   *[]int32 // The set of replicas we are currently adding. (versions: 0+)
	RemovingReplicas *[]int32 // The set of replicas we are currently removing. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ListPartitionReassignmentsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponse.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, res.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
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
func (res *ListPartitionReassignmentsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponse.Read: response or its body is nil")
	}

	*res = ListPartitionReassignmentsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return err
		}
		res.Topics = &topics
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

func (res *ListPartitionReassignmentsResponse) topicsEncoder(w io.Writer, value ListPartitionReassignmentsResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponseTopic.Name must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
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
		return fmt.Errorf("ListPartitionReassignmentsResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
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

func (res *ListPartitionReassignmentsResponse) topicsDecoder(r io.Reader) (ListPartitionReassignmentsResponseTopic, error) {
	listpartitionreassignmentsresponsetopic := ListPartitionReassignmentsResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return listpartitionreassignmentsresponsetopic, err
		}
		listpartitionreassignmentsresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return listpartitionreassignmentsresponsetopic, err
		}
		listpartitionreassignmentsresponsetopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return listpartitionreassignmentsresponsetopic, err
		}
		listpartitionreassignmentsresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return listpartitionreassignmentsresponsetopic, err
		}
		listpartitionreassignmentsresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listpartitionreassignmentsresponsetopic, err
		}
		listpartitionreassignmentsresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return listpartitionreassignmentsresponsetopic, nil
}

func (res *ListPartitionReassignmentsResponse) partitionsEncoder(w io.Writer, value ListPartitionReassignmentsResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// Replicas (versions: 0+)
	if value.Replicas == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponseTopicPartition.Replicas must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Replicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Replicas); err != nil {
			return err
		}
	}

	// AddingReplicas (versions: 0+)
	if value.AddingReplicas == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponseTopicPartition.AddingReplicas must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.AddingReplicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.AddingReplicas); err != nil {
			return err
		}
	}

	// RemovingReplicas (versions: 0+)
	if value.RemovingReplicas == nil {
		return fmt.Errorf("ListPartitionReassignmentsResponseTopicPartition.RemovingReplicas must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.RemovingReplicas); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.RemovingReplicas); err != nil {
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

func (res *ListPartitionReassignmentsResponse) partitionsDecoder(r io.Reader) (ListPartitionReassignmentsResponseTopicPartition, error) {
	listpartitionreassignmentsresponsetopicpartition := ListPartitionReassignmentsResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return listpartitionreassignmentsresponsetopicpartition, err
	}
	listpartitionreassignmentsresponsetopicpartition.PartitionIndex = partitionindex

	// Replicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		replicas, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.Replicas = &replicas
	} else {
		replicas, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.Replicas = &replicas
	}

	// AddingReplicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		addingreplicas, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.AddingReplicas = &addingreplicas
	} else {
		addingreplicas, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.AddingReplicas = &addingreplicas
	}

	// RemovingReplicas (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		removingreplicas, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.RemovingReplicas = &removingreplicas
	} else {
		removingreplicas, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.RemovingReplicas = &removingreplicas
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listpartitionreassignmentsresponsetopicpartition, err
		}
		listpartitionreassignmentsresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return listpartitionreassignmentsresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ListPartitionReassignmentsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ListPartitionReassignmentsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ListPartitionReassignmentsResponseTopic) PrettyPrint() string {
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
func (value *ListPartitionReassignmentsResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)

	if value.Replicas != nil {
		fmt.Fprintf(w, "                Replicas: %v\n", *value.Replicas)
	} else {
		fmt.Fprintf(w, "                Replicas: nil\n")
	}

	if value.AddingReplicas != nil {
		fmt.Fprintf(w, "                AddingReplicas: %v\n", *value.AddingReplicas)
	} else {
		fmt.Fprintf(w, "                AddingReplicas: nil\n")
	}

	if value.RemovingReplicas != nil {
		fmt.Fprintf(w, "                RemovingReplicas: %v\n", *value.RemovingReplicas)
	} else {
		fmt.Fprintf(w, "                RemovingReplicas: nil\n")
	}

	return w.String()
}
