package deleterecords

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteRecordsRequest struct {
	ApiVersion      int16
	Topics          *[]DeleteRecordsRequestTopic // Each topic that we want to delete records from. (versions: 0+)
	TimeoutMs       int32                        // How long to wait for the deletion to complete, in milliseconds. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteRecordsRequestTopic struct {
	Name            *string                               // The topic name. (versions: 0+)
	Partitions      *[]DeleteRecordsRequestTopicPartition // Each partition that we want to delete records from. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DeleteRecordsRequestTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	Offset          int64 // The deletion offset. -1 means that records should be truncated to the high watermark. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DeleteRecordsRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("DeleteRecordsRequest.Topics must not be nil in version %d", req.ApiVersion)
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
func (req *DeleteRecordsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DeleteRecordsRequest.Read: request or its body is nil")
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

func (req *DeleteRecordsRequest) topicsEncoder(w io.Writer, value DeleteRecordsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DeleteRecordsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("DeleteRecordsRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *DeleteRecordsRequest) topicsDecoder(r io.Reader) (DeleteRecordsRequestTopic, error) {
	deleterecordsrequesttopic := DeleteRecordsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return deleterecordsrequesttopic, err
		}
		deleterecordsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return deleterecordsrequesttopic, err
		}
		deleterecordsrequesttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return deleterecordsrequesttopic, err
		}
		deleterecordsrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return deleterecordsrequesttopic, err
		}
		deleterecordsrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleterecordsrequesttopic, err
		}
		deleterecordsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return deleterecordsrequesttopic, nil
}

func (req *DeleteRecordsRequest) partitionsEncoder(w io.Writer, value DeleteRecordsRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// Offset (versions: 0+)
	if err := protocol.WriteInt64(w, value.Offset); err != nil {
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

func (req *DeleteRecordsRequest) partitionsDecoder(r io.Reader) (DeleteRecordsRequestTopicPartition, error) {
	deleterecordsrequesttopicpartition := DeleteRecordsRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return deleterecordsrequesttopicpartition, err
	}
	deleterecordsrequesttopicpartition.PartitionIndex = partitionindex

	// Offset (versions: 0+)
	offset, err := protocol.ReadInt64(r)
	if err != nil {
		return deleterecordsrequesttopicpartition, err
	}
	deleterecordsrequesttopicpartition.Offset = offset

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return deleterecordsrequesttopicpartition, err
		}
		deleterecordsrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return deleterecordsrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DeleteRecordsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DeleteRecordsRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DeleteRecordsRequestTopic) PrettyPrint() string {
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
func (value *DeleteRecordsRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                Offset: %v\n", value.Offset)

	return w.String()
}
