package electleaders

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ElectLeadersRequest struct {
	ApiVersion      int16
	ElectionType    int8                                 // Type of elections to conduct for the partition. A value of '0' elects the preferred replica. A value of '1' elects the first live replica if there are no in-sync replica. (versions: 1+)
	TopicPartitions *[]ElectLeadersRequestTopicPartition // The topic partitions to elect leaders. (versions: 0+, nullable: 0+)
	TimeoutMs       int32                                // The time in ms to wait for the election to complete. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ElectLeadersRequestTopicPartition struct {
	Topic           *string  // The name of a topic. (versions: 0+)
	Partitions      *[]int32 // The partitions of this topic whose leader should be elected. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *ElectLeadersRequest) Write(w io.Writer) error {
	// ElectionType (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, req.ElectionType); err != nil {
			return err
		}
	}

	// TopicPartitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicPartitionsEncoder, req.TopicPartitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.topicPartitionsEncoder, req.TopicPartitions); err != nil {
			return err
		}
	}

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
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
func (req *ElectLeadersRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ElectLeadersRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ElectionType (versions: 1+)
	if req.ApiVersion >= 1 {
		electiontype, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.ElectionType = electiontype
	}

	// TopicPartitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicpartitions, err := protocol.ReadNullableCompactArray(r, req.topicPartitionsDecoder)
		if err != nil {
			return err
		}
		req.TopicPartitions = topicpartitions
	} else {
		topicpartitions, err := protocol.ReadNullableArray(r, req.topicPartitionsDecoder)
		if err != nil {
			return err
		}
		req.TopicPartitions = topicpartitions
	}

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

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

func (req *ElectLeadersRequest) topicPartitionsEncoder(w io.Writer, value ElectLeadersRequestTopicPartition) error {
	// Topic (versions: 0+)
	if value.Topic == nil {
		return fmt.Errorf("ElectLeadersRequestTopicPartition.Topic must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Topic); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Topic); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("ElectLeadersRequestTopicPartition.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (req *ElectLeadersRequest) topicPartitionsDecoder(r io.Reader) (ElectLeadersRequestTopicPartition, error) {
	electleadersrequesttopicpartition := ElectLeadersRequestTopicPartition{}

	// Topic (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topic, err := protocol.ReadCompactString(r)
		if err != nil {
			return electleadersrequesttopicpartition, err
		}
		electleadersrequesttopicpartition.Topic = &topic
	} else {
		topic, err := protocol.ReadString(r)
		if err != nil {
			return electleadersrequesttopicpartition, err
		}
		electleadersrequesttopicpartition.Topic = &topic
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return electleadersrequesttopicpartition, err
		}
		electleadersrequesttopicpartition.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return electleadersrequesttopicpartition, err
		}
		electleadersrequesttopicpartition.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return electleadersrequesttopicpartition, err
		}
		electleadersrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return electleadersrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ElectLeadersRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ElectLeadersRequest:\n")
	fmt.Fprintf(w, "        ElectionType: %v\n", req.ElectionType)

	if req.TopicPartitions != nil {
		fmt.Fprintf(w, "        TopicPartitions:\n")
		for _, topicpartitions := range *req.TopicPartitions {
			fmt.Fprintf(w, "%s", topicpartitions.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        TopicPartitions: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ElectLeadersRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Topic != nil {
		fmt.Fprintf(w, "            Topic: %v\n", *value.Topic)
	} else {
		fmt.Fprintf(w, "            Topic: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "            Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "            Partitions: nil\n")
	}

	return w.String()
}
