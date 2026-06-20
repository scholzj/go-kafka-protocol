package offsetdelete

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetDeleteRequest struct {
	ApiVersion      int16
	GroupId         *string                     // The unique group identifier. (versions: 0+)
	Topics          *[]OffsetDeleteRequestTopic // The topics to delete offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetDeleteRequestTopic struct {
	Name            *string                              // The topic name. (versions: 0+)
	Partitions      *[]OffsetDeleteRequestTopicPartition // Each partition to delete offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetDeleteRequestTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func (req *OffsetDeleteRequest) Write(w io.Writer) error {
	// GroupId (versions: 0+)
	if req.GroupId == nil {
		return fmt.Errorf("OffsetDeleteRequest.GroupId must not be nil in version %d", req.ApiVersion)
	}
	if err := protocol.WriteString(w, *req.GroupId); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("OffsetDeleteRequest.Topics must not be nil in version %d", req.ApiVersion)
	}
	if err := protocol.WriteArray(w, req.topicsEncoder, *req.Topics); err != nil {
		return err
	}

	return nil
}

// TODO: pass version and bytes only
func (req *OffsetDeleteRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("OffsetDeleteRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupId (versions: 0+)
	groupid, err := protocol.ReadString(r)
	if err != nil {
		return err
	}
	req.GroupId = &groupid

	// Topics (versions: 0+)
	topics, err := protocol.ReadArray(r, req.topicsDecoder)
	if err != nil {
		return err
	}
	req.Topics = &topics

	return nil
}

func (req *OffsetDeleteRequest) topicsEncoder(w io.Writer, value OffsetDeleteRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("OffsetDeleteRequestTopic.Name must not be nil in version %d", req.ApiVersion)
	}
	if err := protocol.WriteString(w, *value.Name); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("OffsetDeleteRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if err := protocol.WriteArray(w, req.partitionsEncoder, *value.Partitions); err != nil {
		return err
	}

	return nil
}

func (req *OffsetDeleteRequest) topicsDecoder(r io.Reader) (OffsetDeleteRequestTopic, error) {
	offsetdeleterequesttopic := OffsetDeleteRequestTopic{}

	// Name (versions: 0+)
	name, err := protocol.ReadString(r)
	if err != nil {
		return offsetdeleterequesttopic, err
	}
	offsetdeleterequesttopic.Name = &name

	// Partitions (versions: 0+)
	partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
	if err != nil {
		return offsetdeleterequesttopic, err
	}
	offsetdeleterequesttopic.Partitions = &partitions

	return offsetdeleterequesttopic, nil
}

func (req *OffsetDeleteRequest) partitionsEncoder(w io.Writer, value OffsetDeleteRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	return nil
}

func (req *OffsetDeleteRequest) partitionsDecoder(r io.Reader) (OffsetDeleteRequestTopicPartition, error) {
	offsetdeleterequesttopicpartition := OffsetDeleteRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return offsetdeleterequesttopicpartition, err
	}
	offsetdeleterequesttopicpartition.PartitionIndex = partitionindex

	return offsetdeleterequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *OffsetDeleteRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> OffsetDeleteRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

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
func (value *OffsetDeleteRequestTopic) PrettyPrint() string {
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
func (value *OffsetDeleteRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)

	return w.String()
}
