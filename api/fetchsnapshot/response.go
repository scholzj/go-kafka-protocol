package fetchsnapshot

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type FetchSnapshotResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                // The top level response error code. (versions: 0+)
	Topics          *[]FetchSnapshotResponseTopic        // The topics to fetch. (versions: 0+)
	NodeEndpoints   *[]FetchSnapshotResponseNodeEndpoint // tag 0: Endpoints for all current-leaders enumerated in PartitionSnapshot. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotResponseTopic struct {
	Name            *string                                // The name of the topic to fetch. (versions: 0+)
	Partitions      *[]FetchSnapshotResponseTopicPartition // The partitions to fetch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotResponseTopicPartition struct {
	Index            int32                                             // The partition index. (versions: 0+)
	ErrorCode        int16                                             // The error code, or 0 if there was no fetch error. (versions: 0+)
	SnapshotId       *FetchSnapshotResponseTopicPartitionSnapshotId    // The snapshot endOffset and epoch fetched. (versions: 0+)
	CurrentLeader    *FetchSnapshotResponseTopicPartitionCurrentLeader // tag 0: The leader of the partition at the time of the snapshot. (versions: 0+)
	Size             int64                                             // The total size of the snapshot. (versions: 0+)
	Position         int64                                             // The starting byte position within the snapshot included in the Bytes field. (versions: 0+)
	UnalignedRecords *[]byte                                           // Snapshot data in records format which may not be aligned on an offset boundary. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type FetchSnapshotResponseTopicPartitionSnapshotId struct {
	EndOffset       int64 // The snapshot end offset. (versions: 0+)
	Epoch           int32 // The snapshot epoch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotResponseTopicPartitionCurrentLeader struct {
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown. (versions: 0+)
	LeaderEpoch     int32 // The latest known leader epoch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type FetchSnapshotResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node. (versions: 1+)
	Host            *string // The node's hostname. (versions: 1+)
	Port            uint16  // The node's port. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *FetchSnapshotResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("FetchSnapshotResponse.Topics must not be nil in version %d", res.ApiVersion)
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
		taggedFields, err := res.taggedFieldsEncoder()
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
func (res *FetchSnapshotResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("FetchSnapshotResponse.Read: response or its body is nil")
	}

	*res = FetchSnapshotResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	res.NodeEndpoints = &[]FetchSnapshotResponseNodeEndpoint{}

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
		if err := protocol.ReadTaggedFields(r, res.taggedFieldsDecoder); err != nil {
			return err
		}
	}

	return nil
}

func (res *FetchSnapshotResponse) topicsEncoder(w io.Writer, value FetchSnapshotResponseTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("FetchSnapshotResponseTopic.Name must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("FetchSnapshotResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *FetchSnapshotResponse) topicsDecoder(r io.Reader) (FetchSnapshotResponseTopic, error) {
	fetchsnapshotresponsetopic := FetchSnapshotResponseTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return fetchsnapshotresponsetopic, err
		}
		fetchsnapshotresponsetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return fetchsnapshotresponsetopic, err
		}
		fetchsnapshotresponsetopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return fetchsnapshotresponsetopic, err
		}
		fetchsnapshotresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return fetchsnapshotresponsetopic, err
		}
		fetchsnapshotresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotresponsetopic, err
		}
		fetchsnapshotresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotresponsetopic, nil
}

func (res *FetchSnapshotResponse) partitionsEncoder(w io.Writer, value FetchSnapshotResponseTopicPartition) error {
	// Index (versions: 0+)
	if err := protocol.WriteInt32(w, value.Index); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// SnapshotId (versions: 0+)
	if value.SnapshotId == nil {
		return fmt.Errorf("FetchSnapshotResponseTopicPartition.SnapshotId must not be nil in version %d", res.ApiVersion)
	}
	if err := res.snapshotIdEncoder(w, *value.SnapshotId); err != nil {
		return err
	}

	// CurrentLeader (versions: 0+)
	if !isResponseFlexible(res.ApiVersion) {
		if value.CurrentLeader == nil {
			return fmt.Errorf("FetchSnapshotResponseTopicPartition.CurrentLeader must not be nil in version %d", res.ApiVersion)
		}
		if err := res.currentLeaderEncoder(w, *value.CurrentLeader); err != nil {
			return err
		}
	}

	// Size (versions: 0+)
	if err := protocol.WriteInt64(w, value.Size); err != nil {
		return err
	}

	// Position (versions: 0+)
	if err := protocol.WriteInt64(w, value.Position); err != nil {
		return err
	}

	// UnalignedRecords (versions: 0+)
	if value.UnalignedRecords == nil {
		return fmt.Errorf("FetchSnapshotResponseTopicPartition.UnalignedRecords must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactRecords(w, value.UnalignedRecords); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteRecords(w, value.UnalignedRecords); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		taggedFields, err := res.taggedFieldsEncoderPartitions(value)
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

func (res *FetchSnapshotResponse) partitionsDecoder(r io.Reader) (FetchSnapshotResponseTopicPartition, error) {
	fetchsnapshotresponsetopicpartition := FetchSnapshotResponseTopicPartition{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	fetchsnapshotresponsetopicpartition.CurrentLeader = &FetchSnapshotResponseTopicPartitionCurrentLeader{}

	// Index (versions: 0+)
	index, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartition, err
	}
	fetchsnapshotresponsetopicpartition.Index = index

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartition, err
	}
	fetchsnapshotresponsetopicpartition.ErrorCode = errorcode

	// SnapshotId (versions: 0+)
	snapshotid, err := res.snapshotIdDecoder(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartition, err
	}
	fetchsnapshotresponsetopicpartition.SnapshotId = &snapshotid

	// CurrentLeader (versions: 0+)
	if !isResponseFlexible(res.ApiVersion) {
		currentleader, err := res.currentLeaderDecoder(r)
		if err != nil {
			return fetchsnapshotresponsetopicpartition, err
		}
		fetchsnapshotresponsetopicpartition.CurrentLeader = &currentleader
	}

	// Size (versions: 0+)
	size, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartition, err
	}
	fetchsnapshotresponsetopicpartition.Size = size

	// Position (versions: 0+)
	position, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartition, err
	}
	fetchsnapshotresponsetopicpartition.Position = position

	// UnalignedRecords (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		unalignedrecords, err := protocol.ReadCompactRecordsStrict(r)
		if err != nil {
			return fetchsnapshotresponsetopicpartition, err
		}
		fetchsnapshotresponsetopicpartition.UnalignedRecords = &unalignedrecords
	} else {
		unalignedrecords, err := protocol.ReadRecordsStrict(r)
		if err != nil {
			return fetchsnapshotresponsetopicpartition, err
		}
		fetchsnapshotresponsetopicpartition.UnalignedRecords = &unalignedrecords
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.ReadTaggedFields(r, func(r io.Reader, tag uint64, tagLength uint64) error {
			return res.taggedFieldsDecoderPartitions(r, tag, tagLength, &fetchsnapshotresponsetopicpartition)
		}); err != nil {
			return fetchsnapshotresponsetopicpartition, err
		}
	}

	return fetchsnapshotresponsetopicpartition, nil
}

func (res *FetchSnapshotResponse) taggedFieldsEncoderPartitions(value FetchSnapshotResponseTopicPartition) ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if value.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*value.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if value.CurrentLeader != nil && (value.CurrentLeader.LeaderId != 0 || value.CurrentLeader.LeaderEpoch != 0 || (value.CurrentLeader.rawTaggedFields != nil && len(*value.CurrentLeader.rawTaggedFields) > 0)) {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := res.currentLeaderEncoder(buf, *value.CurrentLeader); err != nil {
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

func (res *FetchSnapshotResponse) taggedFieldsDecoderPartitions(r io.Reader, tag uint64, tagLength uint64, value *FetchSnapshotResponseTopicPartition) error {
	known := false

	switch tag {
	case 0:
		// CurrentLeader
		known = true
		currentleaderVal, err := res.currentLeaderDecoder(r)
		if err != nil {
			return err
		}
		value.CurrentLeader = &currentleaderVal
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

func (res *FetchSnapshotResponse) snapshotIdEncoder(w io.Writer, value FetchSnapshotResponseTopicPartitionSnapshotId) error {
	// EndOffset (versions: 0+)
	if err := protocol.WriteInt64(w, value.EndOffset); err != nil {
		return err
	}

	// Epoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.Epoch); err != nil {
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

func (res *FetchSnapshotResponse) snapshotIdDecoder(r io.Reader) (FetchSnapshotResponseTopicPartitionSnapshotId, error) {
	fetchsnapshotresponsetopicpartitionsnapshotid := FetchSnapshotResponseTopicPartitionSnapshotId{}

	// EndOffset (versions: 0+)
	endoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartitionsnapshotid, err
	}
	fetchsnapshotresponsetopicpartitionsnapshotid.EndOffset = endoffset

	// Epoch (versions: 0+)
	epoch, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartitionsnapshotid, err
	}
	fetchsnapshotresponsetopicpartitionsnapshotid.Epoch = epoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotresponsetopicpartitionsnapshotid, err
		}
		fetchsnapshotresponsetopicpartitionsnapshotid.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotresponsetopicpartitionsnapshotid, nil
}

func (res *FetchSnapshotResponse) currentLeaderEncoder(w io.Writer, value FetchSnapshotResponseTopicPartitionCurrentLeader) error {
	// LeaderId (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderId); err != nil {
		return err
	}

	// LeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.LeaderEpoch); err != nil {
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

func (res *FetchSnapshotResponse) currentLeaderDecoder(r io.Reader) (FetchSnapshotResponseTopicPartitionCurrentLeader, error) {
	fetchsnapshotresponsetopicpartitioncurrentleader := FetchSnapshotResponseTopicPartitionCurrentLeader{}

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartitioncurrentleader, err
	}
	fetchsnapshotresponsetopicpartitioncurrentleader.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return fetchsnapshotresponsetopicpartitioncurrentleader, err
	}
	fetchsnapshotresponsetopicpartitioncurrentleader.LeaderEpoch = leaderepoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotresponsetopicpartitioncurrentleader, err
		}
		fetchsnapshotresponsetopicpartitioncurrentleader.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotresponsetopicpartitioncurrentleader, nil
}

func (res *FetchSnapshotResponse) nodeEndpointsEncoder(w io.Writer, value FetchSnapshotResponseNodeEndpoint) error {
	// NodeId (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 1+)
	if res.ApiVersion >= 1 {
		if value.Host == nil {
			return fmt.Errorf("FetchSnapshotResponseNodeEndpoint.Host must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *value.Host); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *value.Host); err != nil {
				return err
			}
		}
	}

	// Port (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteUint16(w, value.Port); err != nil {
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

func (res *FetchSnapshotResponse) nodeEndpointsDecoder(r io.Reader) (FetchSnapshotResponseNodeEndpoint, error) {
	fetchsnapshotresponsenodeendpoint := FetchSnapshotResponseNodeEndpoint{}

	// NodeId (versions: 1+)
	if res.ApiVersion >= 1 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return fetchsnapshotresponsenodeendpoint, err
		}
		fetchsnapshotresponsenodeendpoint.NodeId = nodeid
	}

	// Host (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return fetchsnapshotresponsenodeendpoint, err
			}
			fetchsnapshotresponsenodeendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return fetchsnapshotresponsenodeendpoint, err
			}
			fetchsnapshotresponsenodeendpoint.Host = &host
		}
	}

	// Port (versions: 1+)
	if res.ApiVersion >= 1 {
		port, err := protocol.ReadUInt16(r)
		if err != nil {
			return fetchsnapshotresponsenodeendpoint, err
		}
		fetchsnapshotresponsenodeendpoint.Port = port
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return fetchsnapshotresponsenodeendpoint, err
		}
		fetchsnapshotresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return fetchsnapshotresponsenodeendpoint, nil
}

func (res *FetchSnapshotResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if res.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*res.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 1+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if res.ApiVersion >= 1 && res.NodeEndpoints != nil && len(*res.NodeEndpoints) > 0 {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactArray(buf, res.nodeEndpointsEncoder, res.NodeEndpoints); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if res.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *res.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (res *FetchSnapshotResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	known := false

	switch tag {
	case 0:
		// NodeEndpoints
		if res.ApiVersion >= 1 {
			known = true
			nodeendpoints, err := protocol.ReadCompactArray(r, res.nodeEndpointsDecoder)
			if err != nil {
				return err
			}
			res.NodeEndpoints = &nodeendpoints
		}
	}

	if !known {
		// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if res.rawTaggedFields == nil {
			rawTaggedFields := make([]protocol.TaggedField, 0)
			res.rawTaggedFields = &rawTaggedFields
		}
		*res.rawTaggedFields = append(*res.rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *FetchSnapshotResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- FetchSnapshotResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *res.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	if res.NodeEndpoints != nil {
		fmt.Fprintf(w, "        NodeEndpoints:\n")
		for _, nodeendpoints := range *res.NodeEndpoints {
			fmt.Fprintf(w, "%s", nodeendpoints.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        NodeEndpoints: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchSnapshotResponseTopic) PrettyPrint() string {
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
func (value *FetchSnapshotResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Index: %v\n", value.Index)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)

	fmt.Fprintf(w, "                SnapshotId:\n")
	if value.SnapshotId != nil {
		fmt.Fprintf(w, "%s", value.SnapshotId.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	fmt.Fprintf(w, "                CurrentLeader:\n")
	if value.CurrentLeader != nil {
		fmt.Fprintf(w, "%s", value.CurrentLeader.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	fmt.Fprintf(w, "                Size: %v\n", value.Size)
	fmt.Fprintf(w, "                Position: %v\n", value.Position)

	if value.UnalignedRecords != nil {
		fmt.Fprintf(w, "                UnalignedRecords: <%d bytes>\n", len(*value.UnalignedRecords))
	} else {
		fmt.Fprintf(w, "                UnalignedRecords: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchSnapshotResponseTopicPartitionSnapshotId) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    EndOffset: %v\n", value.EndOffset)
	fmt.Fprintf(w, "                    Epoch: %v\n", value.Epoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchSnapshotResponseTopicPartitionCurrentLeader) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                    LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *FetchSnapshotResponseNodeEndpoint) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            NodeId: %v\n", value.NodeId)

	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}

	fmt.Fprintf(w, "            Port: %v\n", value.Port)

	return w.String()
}
