package streamsgroupdescribe

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	StreamsGroupDescribeRequestApiKey        = 89
	StreamsGroupDescribeRequestHeaderVersion = 1
)

// StreamsGroupDescribeRequest represents a request message.
type StreamsGroupDescribeRequest struct {
	// The ids of the groups to describe
	GroupIds []string `json:"groupids" versions:"0-999"`
	// Whether to include authorized operations.
	IncludeAuthorizedOperations bool `json:"includeauthorizedoperations" versions:"0-999"`
}

// Encode encodes a StreamsGroupDescribeRequest to a byte slice for the given version.
func (m *StreamsGroupDescribeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a StreamsGroupDescribeRequest from a byte slice for the given version.
func (m *StreamsGroupDescribeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a StreamsGroupDescribeRequest to an io.Writer for the given version.
func (m *StreamsGroupDescribeRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupIds
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactStringArray(w, m.GroupIds); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteStringArray(w, m.GroupIds); err != nil {
				return err
			}
		}
	}
	// IncludeAuthorizedOperations
	if version >= 0 && version <= 999 {
		if err := protocol.WriteBool(w, m.IncludeAuthorizedOperations); err != nil {
			return err
		}
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a StreamsGroupDescribeRequest from an io.Reader for the given version.
func (m *StreamsGroupDescribeRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupIds
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactStringArray(r)
			if err != nil {
				return err
			}
			m.GroupIds = val
		} else {
			val, err := protocol.ReadStringArray(r)
			if err != nil {
				return err
			}
			m.GroupIds = val
		}
	}
	// IncludeAuthorizedOperations
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IncludeAuthorizedOperations = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// Assignment represents .
type Assignment struct {
	// Active tasks for this client.
	ActiveTasks []TaskIds `json:"activetasks" versions:"0-999"`
	// Standby tasks for this client.
	StandbyTasks []TaskIds `json:"standbytasks" versions:"0-999"`
	// Warm-up tasks for this client.
	WarmupTasks []TaskIds `json:"warmuptasks" versions:"0-999"`
}

// TaskIds represents .
type TaskIds struct {
	// The subtopology identifier.
	SubtopologyId string `json:"subtopologyid" versions:"0-999"`
	// The partitions of the input topics processed by this member.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// KeyValue represents .
type KeyValue struct {
	// key of the config
	Key string `json:"key" versions:"0-999"`
	// value of the config
	Value string `json:"value" versions:"0-999"`
}

// TopicInfo represents .
type TopicInfo struct {
	// The name of the topic.
	Name string `json:"name" versions:"0-999"`
	// The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics.
	Partitions int32 `json:"partitions" versions:"0-999"`
	// The replication factor of the topic. Can be 0 if the default replication factor should be used.
	ReplicationFactor int16 `json:"replicationfactor" versions:"0-999"`
	// Topic-level configurations as key-value pairs.
	TopicConfigs []KeyValue `json:"topicconfigs" versions:"0-999"`
}

// Endpoint represents .
type Endpoint struct {
	// host of the endpoint
	Host string `json:"host" versions:"0-999"`
	// port of the endpoint
	Port uint16 `json:"port" versions:"0-999"`
}

// TaskOffset represents .
type TaskOffset struct {
	// The subtopology identifier.
	SubtopologyId string `json:"subtopologyid" versions:"0-999"`
	// The partition.
	Partition int32 `json:"partition" versions:"0-999"`
	// The offset.
	Offset int64 `json:"offset" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for StreamsGroupDescribeRequest.
func (m *StreamsGroupDescribeRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// Write tagged fields count
	if err := protocol.WriteVaruint32(w, uint32(taggedFieldsCount)); err != nil {
		return err
	}

	// Write tagged fields data
	if taggedFieldsCount > 0 {
		if _, err := w.Write(taggedFieldsBuf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

// readTaggedFields reads tagged fields for StreamsGroupDescribeRequest.
func (m *StreamsGroupDescribeRequest) readTaggedFields(r io.Reader, version int16) error {
	// Read tagged fields count
	count, err := protocol.ReadVaruint32(r)
	if err != nil {
		return err
	}

	if count == 0 {
		return nil
	}

	// Read tagged fields
	for i := uint32(0); i < count; i++ {
		tag, err := protocol.ReadVaruint32(r)
		if err != nil {
			return err
		}

		switch tag {
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
