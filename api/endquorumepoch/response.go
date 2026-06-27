package endquorumepoch

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EndQuorumEpochResponse struct {
	ApiVersion      int16
	ErrorCode       int16                                 // The top level error code. (versions: 0+)
	Topics          *[]EndQuorumEpochResponseTopic        // The topic data. (versions: 0+)
	NodeEndpoints   *[]EndQuorumEpochResponseNodeEndpoint // tag 0: Endpoints for all leaders enumerated in PartitionData. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type EndQuorumEpochResponseTopic struct {
	TopicName       *string                                 // The topic name. (versions: 0+)
	Partitions      *[]EndQuorumEpochResponseTopicPartition // The partition data. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type EndQuorumEpochResponseTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The partition level error code. (versions: 0+)
	LeaderId        int32 // The ID of the current leader or -1 if the leader is unknown. (versions: 0+)
	LeaderEpoch     int32 // The latest known leader epoch. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type EndQuorumEpochResponseNodeEndpoint struct {
	NodeId          int32   // The ID of the associated node. (versions: 1+)
	Host            *string // The node's hostname. (versions: 1+)
	Port            uint16  // The node's port. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (res *EndQuorumEpochResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if res.Topics == nil {
		return fmt.Errorf("EndQuorumEpochResponse.Topics must not be nil in version %d", res.ApiVersion)
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
func (res *EndQuorumEpochResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("EndQuorumEpochResponse.Read: response or its body is nil")
	}

	*res = EndQuorumEpochResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	res.NodeEndpoints = &[]EndQuorumEpochResponseNodeEndpoint{}

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

func (res *EndQuorumEpochResponse) topicsEncoder(w io.Writer, value EndQuorumEpochResponseTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("EndQuorumEpochResponseTopic.TopicName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
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
		return fmt.Errorf("EndQuorumEpochResponseTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *EndQuorumEpochResponse) topicsDecoder(r io.Reader) (EndQuorumEpochResponseTopic, error) {
	endquorumepochresponsetopic := EndQuorumEpochResponseTopic{}

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return endquorumepochresponsetopic, err
		}
		endquorumepochresponsetopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return endquorumepochresponsetopic, err
		}
		endquorumepochresponsetopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return endquorumepochresponsetopic, err
		}
		endquorumepochresponsetopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return endquorumepochresponsetopic, err
		}
		endquorumepochresponsetopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochresponsetopic, err
		}
		endquorumepochresponsetopic.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochresponsetopic, nil
}

func (res *EndQuorumEpochResponse) partitionsEncoder(w io.Writer, value EndQuorumEpochResponseTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

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

func (res *EndQuorumEpochResponse) partitionsDecoder(r io.Reader) (EndQuorumEpochResponseTopicPartition, error) {
	endquorumepochresponsetopicpartition := EndQuorumEpochResponseTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochresponsetopicpartition, err
	}
	endquorumepochresponsetopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return endquorumepochresponsetopicpartition, err
	}
	endquorumepochresponsetopicpartition.ErrorCode = errorcode

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochresponsetopicpartition, err
	}
	endquorumepochresponsetopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochresponsetopicpartition, err
	}
	endquorumepochresponsetopicpartition.LeaderEpoch = leaderepoch

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochresponsetopicpartition, err
		}
		endquorumepochresponsetopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochresponsetopicpartition, nil
}

func (res *EndQuorumEpochResponse) nodeEndpointsEncoder(w io.Writer, value EndQuorumEpochResponseNodeEndpoint) error {
	// NodeId (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.NodeId); err != nil {
			return err
		}
	}

	// Host (versions: 1+)
	if res.ApiVersion >= 1 {
		if value.Host == nil {
			return fmt.Errorf("EndQuorumEpochResponseNodeEndpoint.Host must not be nil in version %d", res.ApiVersion)
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

func (res *EndQuorumEpochResponse) nodeEndpointsDecoder(r io.Reader) (EndQuorumEpochResponseNodeEndpoint, error) {
	endquorumepochresponsenodeendpoint := EndQuorumEpochResponseNodeEndpoint{}

	// NodeId (versions: 1+)
	if res.ApiVersion >= 1 {
		nodeid, err := protocol.ReadInt32(r)
		if err != nil {
			return endquorumepochresponsenodeendpoint, err
		}
		endquorumepochresponsenodeendpoint.NodeId = nodeid
	}

	// Host (versions: 1+)
	if res.ApiVersion >= 1 {
		if isResponseFlexible(res.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return endquorumepochresponsenodeendpoint, err
			}
			endquorumepochresponsenodeendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return endquorumepochresponsenodeendpoint, err
			}
			endquorumepochresponsenodeendpoint.Host = &host
		}
	}

	// Port (versions: 1+)
	if res.ApiVersion >= 1 {
		port, err := protocol.ReadUInt16(r)
		if err != nil {
			return endquorumepochresponsenodeendpoint, err
		}
		endquorumepochresponsenodeendpoint.Port = port
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochresponsenodeendpoint, err
		}
		endquorumepochresponsenodeendpoint.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochresponsenodeendpoint, nil
}

func (res *EndQuorumEpochResponse) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
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

func (res *EndQuorumEpochResponse) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
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
func (res *EndQuorumEpochResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- EndQuorumEpochResponse:\n")
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
func (value *EndQuorumEpochResponseTopic) PrettyPrint() string {
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
func (value *EndQuorumEpochResponseTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                ErrorCode: %v\n", value.ErrorCode)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *EndQuorumEpochResponseNodeEndpoint) PrettyPrint() string {
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
