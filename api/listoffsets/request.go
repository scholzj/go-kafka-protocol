package listoffsets

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListOffsetsRequest struct {
	ApiVersion      int16
	ReplicaId       int32                      // The broker ID of the requester, or -1 if this request is being made by a normal consumer. (versions: 0+)
	IsolationLevel  int8                       // This setting controls the visibility of transactional records. Using READ_UNCOMMITTED (isolation_level = 0) makes all records visible. With READ_COMMITTED (isolation_level = 1), non-transactional and COMMITTED transactional records are visible. To be more concrete, READ_COMMITTED returns all data from offsets smaller than the current LSO (last stable offset), and enables the inclusion of the list of aborted transactions in the result, which allows consumers to discard ABORTED transactional records. (versions: 2+)
	Topics          *[]ListOffsetsRequestTopic // Each topic in the request. (versions: 0+)
	TimeoutMs       int32                      // The timeout to await a response in milliseconds for requests that require reading from remote storage for topics enabled with tiered storage. (versions: 10+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListOffsetsRequestTopic struct {
	Name            *string                             // The topic name. (versions: 0+)
	Partitions      *[]ListOffsetsRequestTopicPartition // Each partition in the request. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListOffsetsRequestTopicPartition struct {
	PartitionIndex     int32 // The partition index. (versions: 0+)
	CurrentLeaderEpoch int32 // The current leader epoch. (versions: 4+)
	Timestamp          int64 // The current timestamp. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (req *ListOffsetsRequest) Write(w io.Writer) error {
	// ReplicaId (versions: 0+)
	if err := protocol.WriteInt32(w, req.ReplicaId); err != nil {
		return err
	}

	// IsolationLevel (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteInt8(w, req.IsolationLevel); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("ListOffsetsRequest.Topics must not be nil in version %d", req.ApiVersion)
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

	// TimeoutMs (versions: 10+)
	if req.ApiVersion >= 10 {
		if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
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
func (req *ListOffsetsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ListOffsetsRequest.Read: request or its body is nil")
	}

	*req = ListOffsetsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ReplicaId (versions: 0+)
	replicaid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.ReplicaId = replicaid

	// IsolationLevel (versions: 2+)
	if req.ApiVersion >= 2 {
		isolationlevel, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.IsolationLevel = isolationlevel
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

	// TimeoutMs (versions: 10+)
	if req.ApiVersion >= 10 {
		timeoutms, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.TimeoutMs = timeoutms
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

func (req *ListOffsetsRequest) topicsEncoder(w io.Writer, value ListOffsetsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ListOffsetsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("ListOffsetsRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *ListOffsetsRequest) topicsDecoder(r io.Reader) (ListOffsetsRequestTopic, error) {
	listoffsetsrequesttopic := ListOffsetsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return listoffsetsrequesttopic, err
		}
		listoffsetsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return listoffsetsrequesttopic, err
		}
		listoffsetsrequesttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return listoffsetsrequesttopic, err
		}
		listoffsetsrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return listoffsetsrequesttopic, err
		}
		listoffsetsrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listoffsetsrequesttopic, err
		}
		listoffsetsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return listoffsetsrequesttopic, nil
}

func (req *ListOffsetsRequest) partitionsEncoder(w io.Writer, value ListOffsetsRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// CurrentLeaderEpoch (versions: 4+)
	if req.ApiVersion >= 4 {
		if err := protocol.WriteInt32(w, value.CurrentLeaderEpoch); err != nil {
			return err
		}
	}

	// Timestamp (versions: 0+)
	if err := protocol.WriteInt64(w, value.Timestamp); err != nil {
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

func (req *ListOffsetsRequest) partitionsDecoder(r io.Reader) (ListOffsetsRequestTopicPartition, error) {
	listoffsetsrequesttopicpartition := ListOffsetsRequestTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	listoffsetsrequesttopicpartition.CurrentLeaderEpoch = -1

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return listoffsetsrequesttopicpartition, err
	}
	listoffsetsrequesttopicpartition.PartitionIndex = partitionindex

	// CurrentLeaderEpoch (versions: 4+)
	if req.ApiVersion >= 4 {
		currentleaderepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return listoffsetsrequesttopicpartition, err
		}
		listoffsetsrequesttopicpartition.CurrentLeaderEpoch = currentleaderepoch
	}

	// Timestamp (versions: 0+)
	timestamp, err := protocol.ReadInt64(r)
	if err != nil {
		return listoffsetsrequesttopicpartition, err
	}
	listoffsetsrequesttopicpartition.Timestamp = timestamp

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listoffsetsrequesttopicpartition, err
		}
		listoffsetsrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return listoffsetsrequesttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ListOffsetsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ListOffsetsRequest:\n")
	fmt.Fprintf(w, "        ReplicaId: %v\n", req.ReplicaId)
	fmt.Fprintf(w, "        IsolationLevel: %v\n", req.IsolationLevel)

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
func (value *ListOffsetsRequestTopic) PrettyPrint() string {
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
func (value *ListOffsetsRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                CurrentLeaderEpoch: %v\n", value.CurrentLeaderEpoch)
	fmt.Fprintf(w, "                Timestamp: %v\n", value.Timestamp)

	return w.String()
}
