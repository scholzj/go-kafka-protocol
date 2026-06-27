package describetopicpartitions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeTopicPartitionsRequest struct {
	ApiVersion             int16
	Topics                 *[]DescribeTopicPartitionsRequestTopic // The topics to fetch details for. (versions: 0+)
	ResponsePartitionLimit int32                                  // The maximum number of partitions included in the response. (versions: 0+)
	Cursor                 *DescribeTopicPartitionsRequestCursor  // The first topic and partition index to fetch details for. (versions: 0+, nullable: 0+)
	rawTaggedFields        *[]protocol.TaggedField
}

type DescribeTopicPartitionsRequestTopic struct {
	Name            *string // The topic name. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeTopicPartitionsRequestCursor struct {
	TopicName       *string // The name for the first topic to process. (versions: 0+)
	PartitionIndex  int32   // The partition index to start with. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeTopicPartitionsRequest) Write(w io.Writer) error {
	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("DescribeTopicPartitionsRequest.Topics must not be nil in version %d", req.ApiVersion)
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

	// ResponsePartitionLimit (versions: 0+)
	if err := protocol.WriteInt32(w, req.ResponsePartitionLimit); err != nil {
		return err
	}

	// Cursor (versions: 0+)
	if req.Cursor == nil {
		if err := protocol.WriteInt8(w, -1); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteInt8(w, 1); err != nil {
			return err
		}
		if err := req.cursorEncoder(w, *req.Cursor); err != nil {
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
func (req *DescribeTopicPartitionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeTopicPartitionsRequest.Read: request or its body is nil")
	}

	*req = DescribeTopicPartitionsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.ResponsePartitionLimit = 2000

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

	// ResponsePartitionLimit (versions: 0+)
	responsepartitionlimit, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.ResponsePartitionLimit = responsepartitionlimit

	// Cursor (versions: 0+)
	cursorFlag, err := protocol.ReadInt8(r)
	if err != nil {
		return err
	}
	if cursorFlag >= 0 {
		cursor, err := req.cursorDecoder(r)
		if err != nil {
			return err
		}
		req.Cursor = &cursor
	} else {
		req.Cursor = nil
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

func (req *DescribeTopicPartitionsRequest) topicsEncoder(w io.Writer, value DescribeTopicPartitionsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeTopicPartitionsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeTopicPartitionsRequest) topicsDecoder(r io.Reader) (DescribeTopicPartitionsRequestTopic, error) {
	describetopicpartitionsrequesttopic := DescribeTopicPartitionsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetopicpartitionsrequesttopic, err
		}
		describetopicpartitionsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describetopicpartitionsrequesttopic, err
		}
		describetopicpartitionsrequesttopic.Name = &name
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetopicpartitionsrequesttopic, err
		}
		describetopicpartitionsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return describetopicpartitionsrequesttopic, nil
}

func (req *DescribeTopicPartitionsRequest) cursorEncoder(w io.Writer, value DescribeTopicPartitionsRequestCursor) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeTopicPartitionsRequestCursor.TopicName must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeTopicPartitionsRequest) cursorDecoder(r io.Reader) (DescribeTopicPartitionsRequestCursor, error) {
	describetopicpartitionsrequestcursor := DescribeTopicPartitionsRequestCursor{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describetopicpartitionsrequestcursor, err
		}
		describetopicpartitionsrequestcursor.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describetopicpartitionsrequestcursor, err
		}
		describetopicpartitionsrequestcursor.TopicName = &topicname
	}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describetopicpartitionsrequestcursor, err
	}
	describetopicpartitionsrequestcursor.PartitionIndex = partitionindex

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describetopicpartitionsrequestcursor, err
		}
		describetopicpartitionsrequestcursor.rawTaggedFields = &rawTaggedFields
	}

	return describetopicpartitionsrequestcursor, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeTopicPartitionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeTopicPartitionsRequest:\n")

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	fmt.Fprintf(w, "        ResponsePartitionLimit: %v\n", req.ResponsePartitionLimit)

	fmt.Fprintf(w, "        Cursor:\n")
	if req.Cursor != nil {
		fmt.Fprintf(w, "%s", req.Cursor.PrettyPrint())
	} else {
		fmt.Fprintf(w, "            nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTopicPartitionsRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeTopicPartitionsRequestCursor) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "            TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "            TopicName: nil\n")
	}

	fmt.Fprintf(w, "            PartitionIndex: %v\n", value.PartitionIndex)

	return w.String()
}
