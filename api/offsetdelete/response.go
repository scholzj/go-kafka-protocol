package offsetdelete

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetDeleteResponse struct {
	ApiVersion      int16
	ErrorCode       int16                        // The top-level error code, or 0 if there was no error. (versions: 0+)
	ThrottleTimeMs  int32                        // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Topics          *[]OffsetDeleteResponseTopic // The responses for each topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetDeleteResponseTopic struct {
	Name            *string                               // The topic name. (versions: 0+)
	Partitions      *[]OffsetDeleteResponseTopicPartition // The responses for each partition in the topic. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetDeleteResponseTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The error code, or 0 if there was no error. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func (res *OffsetDeleteResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("OffsetDeleteResponse.Topics must not be nil in version %d", res.ApiVersion)
	}
	if err := protocol.WriteArray(w, res.topicsEncoder, *res.Topics); err != nil {
		return err
	}

	return nil
}

// TODO: pass version and bytes only
func (res *OffsetDeleteResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("OffsetDeleteResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Topics (versions: 0+)
	topics, err := protocol.ReadArray(r, res.topicsDecoder)
	if err != nil {
		return err
	}
	res.Topics = &topics

	return nil
}

func (res *OffsetDeleteResponse) topicsEncoder(w io.Writer, value OffsetDeleteResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("OffsetDeleteResponseTopic.Name must not be nil in version %d", res.ApiVersion)
	}
	if err := protocol.WriteString(w, *value.Name); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("OffsetDeleteResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
		return err
	}

	return nil
}

func (res *OffsetDeleteResponse) topicsDecoder(r io.Reader) (OffsetDeleteResponseTopic, error) {
	offsetdeleteresponsetopic := OffsetDeleteResponseTopic{}

	// Name (versions: 0+)
	name, err := protocol.ReadString(r)
	if err != nil {
		return offsetdeleteresponsetopic, err
	}
	offsetdeleteresponsetopic.Name = &name

	// Partitions (versions: 0+)
	partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
	if err != nil {
		return offsetdeleteresponsetopic, err
	}
	offsetdeleteresponsetopic.Partitions = &partitions

	return offsetdeleteresponsetopic, nil
}

func (res *OffsetDeleteResponse) partitionsEncoder(w io.Writer, value OffsetDeleteResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	return nil
}

func (res *OffsetDeleteResponse) partitionsDecoder(r io.Reader) (OffsetDeleteResponseTopicPartition, error) {
	offsetdeleteresponsetopicpartition := OffsetDeleteResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetdeleteresponsetopicpartition, err
	}
	offsetdeleteresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return offsetdeleteresponsetopicpartition, err
	}
	offsetdeleteresponsetopicpartition.ErrorCode = errorcode

	return offsetdeleteresponsetopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *OffsetDeleteResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- OffsetDeleteResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
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
func (value *OffsetDeleteResponseTopic) PrettyPrint() string {
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
func (value *OffsetDeleteResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
