package fetchsnapshot

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type FetchSnapshotRequest struct {
	ApiVersion      int16
	ClusterId       *string                      // tag 0: The clusterId if known, this is used to validate metadata fetches prior to broker registration. (versions: 0+, nullable: 0+)
	ReplicaId       int32                        // The broker ID of the follower. (versions: 0+)
	MaxBytes        int32                        // The maximum bytes to fetch from all of the snapshots. (versions: 0+)
	Topics          *[]FetchSnapshotRequestTopic // The topics to fetch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotRequestTopic struct {
	Name            *string                               // The name of the topic to fetch. (versions: 0+)
	Partitions      *[]FetchSnapshotRequestTopicPartition // The partitions to fetch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotRequestTopicPartition struct {
	Partition          int32                                         // The partition index. (versions: 0+)
	CurrentLeaderEpoch int32                                         // The current leader epoch of the partition, -1 for unknown leader epoch. (versions: 0+)
	SnapshotId         *FetchSnapshotRequestTopicPartitionSnapshotId // The snapshot endOffset and epoch to fetch. (versions: 0+)
	Position           int64                                         // The byte position within the snapshot to start fetching from. (versions: 0+)
	ReplicaDirectoryId uuid.UUID                                     // tag 0: The directory id of the follower fetching. (versions: 1+)
	rawTaggedFields    *[]protocol.TaggedField
}

type FetchSnapshotRequestTopicPartitionSnapshotId struct {
	EndOffset       int64 // The end offset of the snapshot. (versions: 0+)
	Epoch           int32 // The epoch of the snapshot. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *FetchSnapshotRequest) Write(w io.Writer) error {
	// ReplicaId (versions: 0+)
	if err := protocol.WriteInt32(w, req.ReplicaId); err != nil {
		return err
	}

	// MaxBytes (versions: 0+)
	if err := protocol.WriteInt32(w, req.MaxBytes); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("FetchSnapshotRequest.Topics must not be nil in version %d", req.ApiVersion)
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
		taggedFields, err := req.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *FetchSnapshotRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("FetchSnapshotRequest.Read: request or its body is nil")
	}

	*req = FetchSnapshotRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.ReplicaId = -1
	req.MaxBytes = 0x7fffffff

	// ReplicaId (versions: 0+)
	replicaid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.ReplicaId = replicaid

	// MaxBytes (versions: 0+)
	maxbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.MaxBytes = maxbytes

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
		if err := protocol.ReadTaggedFields(r, req.taggedFieldsDecoder); err != nil {
			return err
		}
	}

	return nil
}

func (req *FetchSnapshotRequest) topicsEncoder(w io.Writer, value FetchSnapshotRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("FetchSnapshotRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("FetchSnapshotRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *FetchSnapshotRequest) topicsDecoder(r io.Reader) (FetchSnapshotRequestTopic, error) {
	fetchsnapshotrequesttopic := FetchSnapshotRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return fetchsnapshotrequesttopic, err
		}
		fetchsnapshotrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return fetchsnapshotrequesttopic, err
		}
		fetchsnapshotrequesttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return fetchsnapshotrequesttopic, err
		}
		fetchsnapshotrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return fetchsnapshotrequesttopic, err
		}
		fetchsnapshotrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotrequesttopic, err
		}
		fetchsnapshotrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotrequesttopic, nil
}

func (req *FetchSnapshotRequest) partitionsEncoder(w io.Writer, value FetchSnapshotRequestTopicPartition) error {
	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// CurrentLeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.CurrentLeaderEpoch); err != nil {
		return err
	}

	// SnapshotId (versions: 0+)
	if value.SnapshotId == nil {
		return fmt.Errorf("FetchSnapshotRequestTopicPartition.SnapshotId must not be nil in version %d", req.ApiVersion)
	}
	if err := req.snapshotIdEncoder(w, *value.SnapshotId); err != nil {
		return err
	}

	// Position (versions: 0+)
	if err := protocol.WriteInt64(w, value.Position); err != nil {
		return err
	}

	// ReplicaDirectoryId (versions: 1+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 1 {
			if err := protocol.WriteUUID(w, value.ReplicaDirectoryId); err != nil {
				return err
			}
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		taggedFields, err := req.taggedFieldsEncoderPartitions(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (req *FetchSnapshotRequest) partitionsDecoder(r io.Reader) (FetchSnapshotRequestTopicPartition, error) {
	fetchsnapshotrequesttopicpartition := FetchSnapshotRequestTopicPartition{}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartition, err
	}
	fetchsnapshotrequesttopicpartition.Partition = partition

	// CurrentLeaderEpoch (versions: 0+)
	currentleaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartition, err
	}
	fetchsnapshotrequesttopicpartition.CurrentLeaderEpoch = currentleaderepoch

	// SnapshotId (versions: 0+)
	snapshotid, err := req.snapshotIdDecoder(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartition, err
	}
	fetchsnapshotrequesttopicpartition.SnapshotId = &snapshotid

	// Position (versions: 0+)
	position, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartition, err
	}
	fetchsnapshotrequesttopicpartition.Position = position

	// ReplicaDirectoryId (versions: 1+)
	if !isRequestFlexible(req.ApiVersion) {
		if req.ApiVersion >= 1 {
			replicadirectoryid, err := protocol.ReadUUID(r)
			if err != nil {
				return fetchsnapshotrequesttopicpartition, err
			}
			fetchsnapshotrequesttopicpartition.ReplicaDirectoryId = replicadirectoryid
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return req.taggedFieldsDecoderPartitions(r, tag, tagLength, &fetchsnapshotrequesttopicpartition)
		}); err != nil {
			return fetchsnapshotrequesttopicpartition, err
		}
	}

	return fetchsnapshotrequesttopicpartition, nil
}

func (req *FetchSnapshotRequest) taggedFieldsEncoderPartitions(value FetchSnapshotRequestTopicPartition) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if req.ApiVersion >= 1 && value.ReplicaDirectoryId != (uuid.UUID{}) {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteUUID(buf, value.ReplicaDirectoryId); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if value.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *value.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (req *FetchSnapshotRequest) taggedFieldsDecoderPartitions(r io.Reader, tag uint64, tagLength uint64, value *FetchSnapshotRequestTopicPartition) error {
	known := false

	switch tag {
	case 0:
		// ReplicaDirectoryId
		if req.ApiVersion >= 1 {
			known = true
			replicadirectoryid, err := protocol.ReadUUID(r)
			if err != nil {
				return err
			}
			value.ReplicaDirectoryId = replicadirectoryid
		}
	}

	if !known {
		// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if value.rawTaggedFields == nil {
			rawTaggedFields := make([]protocol.TaggedField, 0)
			value.rawTaggedFields = &rawTaggedFields
		}
		*value.rawTaggedFields = append(*value.rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	return nil
}

func (req *FetchSnapshotRequest) snapshotIdEncoder(w io.Writer, value FetchSnapshotRequestTopicPartitionSnapshotId) error {
	// EndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.EndOffset); err != nil {
		return err
	}

	// Epoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.Epoch); err != nil {
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

func (req *FetchSnapshotRequest) snapshotIdDecoder(r io.Reader) (FetchSnapshotRequestTopicPartitionSnapshotId, error) {
	fetchsnapshotrequesttopicpartitionsnapshotid := FetchSnapshotRequestTopicPartitionSnapshotId{}

	// EndOffset (versions: 0+)
	endoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartitionsnapshotid, err
	}
	fetchsnapshotrequesttopicpartitionsnapshotid.EndOffset = endoffset

	// Epoch (versions: 0+)
	epoch, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotrequesttopicpartitionsnapshotid, err
	}
	fetchsnapshotrequesttopicpartitionsnapshotid.Epoch = epoch

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotrequesttopicpartitionsnapshotid, err
		}
		fetchsnapshotrequesttopicpartitionsnapshotid.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotrequesttopicpartitionsnapshotid, nil
}

func (req *FetchSnapshotRequest) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if req.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*req.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if req.ClusterId != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactString(buf, req.ClusterId); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if req.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *req.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (req *FetchSnapshotRequest) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	known := false

	switch tag {
	case 0:
		// ClusterId
		known = true
		clusterid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	}

	if !known {
		// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if req.rawTaggedFields == nil {
			rawTaggedFields := make([]protocol.TaggedField, 0)
			req.rawTaggedFields = &rawTaggedFields
		}
		*req.rawTaggedFields = append(*req.rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *FetchSnapshotRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> FetchSnapshotRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        ReplicaId: %v\n", req.ReplicaId)
	fmt.Fprintf(w, "        MaxBytes: %v\n", req.MaxBytes)

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
func (value *FetchSnapshotRequestTopic) PrettyPrint() string {
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
func (value *FetchSnapshotRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                CurrentLeaderEpoch: %v\n", value.CurrentLeaderEpoch)

	fmt.Fprintf(w, "                SnapshotId:\n")
	if value.SnapshotId != nil {
		fmt.Fprintf(w, "%s", value.SnapshotId.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	fmt.Fprintf(w, "                Position: %v\n", value.Position)
	fmt.Fprintf(w, "                ReplicaDirectoryId: %v\n", value.ReplicaDirectoryId)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchSnapshotRequestTopicPartitionSnapshotId) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    EndOffset: %v\n", value.EndOffset)
	fmt.Fprintf(w, "                    Epoch: %v\n", value.Epoch)

	return w.String()
}
