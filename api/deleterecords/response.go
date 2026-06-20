package deleterecords

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteRecordsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                         // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Topics          *[]DeleteRecordsResponseTopic // Each topic that we wanted to delete records from. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteRecordsResponseTopic struct {
	Name            *string                                // The topic name. (versions: 0+)
	Partitions      *[]DeleteRecordsResponseTopicPartition // Each partition that we wanted to delete records from. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteRecordsResponseTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	LowWatermark    int64 // The partition low water mark. (versions: 0+)
	ErrorCode       int16 // The deletion error code, or 0 if the deletion succeeded. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *DeleteRecordsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("DeleteRecordsResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *DeleteRecordsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DeleteRecordsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
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

func (res *DeleteRecordsResponse) topicsEncoder(w io.Writer, value DeleteRecordsResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DeleteRecordsResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("DeleteRecordsResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *DeleteRecordsResponse) topicsDecoder(r io.Reader) (DeleteRecordsResponseTopic, error) {
	deleterecordsresponsetopic := DeleteRecordsResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return deleterecordsresponsetopic, err
		}
		deleterecordsresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return deleterecordsresponsetopic, err
		}
		deleterecordsresponsetopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return deleterecordsresponsetopic, err
		}
		deleterecordsresponsetopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return deleterecordsresponsetopic, err
		}
		deleterecordsresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleterecordsresponsetopic, err
		}
		deleterecordsresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return deleterecordsresponsetopic, nil
}

func (res *DeleteRecordsResponse) partitionsEncoder(w io.Writer, value DeleteRecordsResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// LowWatermark (versions: 0+)
	if err := protocol.WriteInt64(w, value.LowWatermark); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
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

func (res *DeleteRecordsResponse) partitionsDecoder(r io.Reader) (DeleteRecordsResponseTopicPartition, error) {
	deleterecordsresponsetopicpartition := DeleteRecordsResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return deleterecordsresponsetopicpartition, err
	}
	deleterecordsresponsetopicpartition.PartitionIndex = partitionindex

	// LowWatermark (versions: 0+)
	lowwatermark, err := protocol.ReadInt64(r)
	if err != nil {
		return deleterecordsresponsetopicpartition, err
	}
	deleterecordsresponsetopicpartition.LowWatermark = lowwatermark

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return deleterecordsresponsetopicpartition, err
	}
	deleterecordsresponsetopicpartition.ErrorCode = errorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleterecordsresponsetopicpartition, err
		}
		deleterecordsresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return deleterecordsresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DeleteRecordsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DeleteRecordsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

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
func (value *DeleteRecordsResponseTopic) PrettyPrint() string {
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
func (value *DeleteRecordsResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                LowWatermark: %v\n", value.LowWatermark)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
