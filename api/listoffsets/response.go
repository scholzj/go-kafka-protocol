package listoffsets

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListOffsetsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                       // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 2+)
	Topics          *[]ListOffsetsResponseTopic // Each topic in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListOffsetsResponseTopic struct {
	Name            *string                              // The topic name. (versions: 0+)
	Partitions      *[]ListOffsetsResponseTopicPartition // Each partition in the response. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListOffsetsResponseTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The partition error code, or 0 if there was no error. (versions: 0+)
	Timestamp       int64 // The timestamp associated with the returned offset. (versions: 1+)
	Offset          int64 // The returned offset. (versions: 1+)
	LeaderEpoch     int32 // The leader epoch associated with the returned offset. (versions: 4+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (res *ListOffsetsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("ListOffsetsResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *ListOffsetsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ListOffsetsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 2+)
	if res.ApiVersion >= 2 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

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

func (res *ListOffsetsResponse) topicsEncoder(w io.Writer, value ListOffsetsResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ListOffsetsResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("ListOffsetsResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *ListOffsetsResponse) topicsDecoder(r io.Reader) (ListOffsetsResponseTopic, error) {
	listoffsetsresponsetopic := ListOffsetsResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return listoffsetsresponsetopic, err
		}
		listoffsetsresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return listoffsetsresponsetopic, err
		}
		listoffsetsresponsetopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return listoffsetsresponsetopic, err
		}
		listoffsetsresponsetopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return listoffsetsresponsetopic, err
		}
		listoffsetsresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listoffsetsresponsetopic, err
		}
		listoffsetsresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return listoffsetsresponsetopic, nil
}

func (res *ListOffsetsResponse) partitionsEncoder(w io.Writer, value ListOffsetsResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// Timestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.Timestamp); err != nil {
			return err
		}
	}

	// Offset (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt64(w, value.Offset); err != nil {
			return err
		}
	}

	// LeaderEpoch (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
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

func (res *ListOffsetsResponse) partitionsDecoder(r io.Reader) (ListOffsetsResponseTopicPartition, error) {
	listoffsetsresponsetopicpartition := ListOffsetsResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return listoffsetsresponsetopicpartition, err
	}
	listoffsetsresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return listoffsetsresponsetopicpartition, err
	}
	listoffsetsresponsetopicpartition.ErrorCode = errorcode

	// Timestamp (versions: 1+)
	if res.ApiVersion >= 1 {
		timestamp, err := protocol.ReadInt64(r)
		if err != nil {
			return listoffsetsresponsetopicpartition, err
		}
		listoffsetsresponsetopicpartition.Timestamp = timestamp
	}

	// Offset (versions: 1+)
	if res.ApiVersion >= 1 {
		offset, err := protocol.ReadInt64(r)
		if err != nil {
			return listoffsetsresponsetopicpartition, err
		}
		listoffsetsresponsetopicpartition.Offset = offset
	}

	// LeaderEpoch (versions: 4+)
	if res.ApiVersion >= 4 {
		leaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return listoffsetsresponsetopicpartition, err
		}
		listoffsetsresponsetopicpartition.LeaderEpoch = leaderepoch
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listoffsetsresponsetopicpartition, err
		}
		listoffsetsresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return listoffsetsresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ListOffsetsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ListOffsetsResponse:\n")
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
func (value *ListOffsetsResponseTopic) PrettyPrint() string {
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
func (value *ListOffsetsResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                Timestamp: %v\n", value.Timestamp)
	fmt.Fprintf(w, "                Offset: %v\n", value.Offset)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}
