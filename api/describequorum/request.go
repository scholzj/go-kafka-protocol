package describequorum

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeQuorumRequest struct {
	ApiVersion      int16
	Topics          *[]DescribeQuorumRequestTopic // The topics to describe. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumRequestTopic struct {
	TopicName       *string                                // The topic name. (versions: 0+)
	Partitions      *[]DescribeQuorumRequestTopicPartition // The partitions to describe. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeQuorumRequestTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeQuorumRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("DescribeQuorumRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *DescribeQuorumRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeQuorumRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
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

func (req *DescribeQuorumRequest) topicsEncoder(w io.Writer, value DescribeQuorumRequestTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeQuorumRequestTopic.TopicName must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.TopicName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.TopicName); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("DescribeQuorumRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeQuorumRequest) topicsDecoder(r io.Reader) (DescribeQuorumRequestTopic, error) {
	describequorumrequesttopic := DescribeQuorumRequestTopic{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describequorumrequesttopic, err
		}
		describequorumrequesttopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describequorumrequesttopic, err
		}
		describequorumrequesttopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return describequorumrequesttopic, err
		}
		describequorumrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return describequorumrequesttopic, err
		}
		describequorumrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumrequesttopic, err
		}
		describequorumrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return describequorumrequesttopic, nil
}

func (req *DescribeQuorumRequest) partitionsEncoder(w io.Writer, value DescribeQuorumRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
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

func (req *DescribeQuorumRequest) partitionsDecoder(r io.Reader) (DescribeQuorumRequestTopicPartition, error) {
	describequorumrequesttopicpartition := DescribeQuorumRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describequorumrequesttopicpartition, err
	}
	describequorumrequesttopicpartition.PartitionIndex = partitionindex

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describequorumrequesttopicpartition, err
		}
		describequorumrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describequorumrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeQuorumRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeQuorumRequest:\n")

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
func (value *DescribeQuorumRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "            TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "            TopicName: nil\n")
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
func (value *DescribeQuorumRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)

	return w.String()
}
