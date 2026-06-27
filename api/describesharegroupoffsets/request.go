package describesharegroupoffsets

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeShareGroupOffsetsRequest struct {
	ApiVersion      int16
	Groups          *[]DescribeShareGroupOffsetsRequestGroup // The groups to describe offsets for. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeShareGroupOffsetsRequestGroup struct {
	GroupId         *string                                       // The group identifier. (versions: 0+)
	Topics          *[]DescribeShareGroupOffsetsRequestGroupTopic // The topics to describe offsets for, or null for all topic-partitions. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeShareGroupOffsetsRequestGroupTopic struct {
	TopicName       *string  // The topic name. (versions: 0+)
	Partitions      *[]int32 // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeShareGroupOffsetsRequest) Write(w io.Writer) error {
	// Groups (versions: 0+)
	if req.Groups == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsRequest.Groups must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.groupsEncoder, req.Groups); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.groupsEncoder, *req.Groups); err != nil {
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
func (req *DescribeShareGroupOffsetsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsRequest.Read: request or its body is nil")
	}

	*req = DescribeShareGroupOffsetsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Groups (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groups, err := protocol.ReadCompactArray(r, req.groupsDecoder)
		if err != nil {
			return err
		}
		req.Groups = &groups
	} else {
		groups, err := protocol.ReadArray(r, req.groupsDecoder)
		if err != nil {
			return err
		}
		req.Groups = &groups
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

func (req *DescribeShareGroupOffsetsRequest) groupsEncoder(w io.Writer, value DescribeShareGroupOffsetsRequestGroup) error {
	// GroupId (versions: 0+)
	if value.GroupId == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsRequestGroup.GroupId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.topicsEncoder, value.Topics); err != nil {
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

func (req *DescribeShareGroupOffsetsRequest) groupsDecoder(r io.Reader) (DescribeShareGroupOffsetsRequestGroup, error) {
	describesharegroupoffsetsrequestgroup := DescribeShareGroupOffsetsRequestGroup{}

	// GroupId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return describesharegroupoffsetsrequestgroup, err
		}
		describesharegroupoffsetsrequestgroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return describesharegroupoffsetsrequestgroup, err
		}
		describesharegroupoffsetsrequestgroup.GroupId = &groupid
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return describesharegroupoffsetsrequestgroup, err
		}
		describesharegroupoffsetsrequestgroup.Topics = topics
	} else {
		topics, err := protocol.ReadNullableArray(r, req.topicsDecoder)
		if err != nil {
			return describesharegroupoffsetsrequestgroup, err
		}
		describesharegroupoffsetsrequestgroup.Topics = topics
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describesharegroupoffsetsrequestgroup, err
		}
		describesharegroupoffsetsrequestgroup.rawTaggedFields = &rawTaggedFields
	}

	return describesharegroupoffsetsrequestgroup, nil
}

func (req *DescribeShareGroupOffsetsRequest) topicsEncoder(w io.Writer, value DescribeShareGroupOffsetsRequestGroupTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("DescribeShareGroupOffsetsRequestGroupTopic.TopicName must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("DescribeShareGroupOffsetsRequestGroupTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeShareGroupOffsetsRequest) topicsDecoder(r io.Reader) (DescribeShareGroupOffsetsRequestGroupTopic, error) {
	describesharegroupoffsetsrequestgrouptopic := DescribeShareGroupOffsetsRequestGroupTopic{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describesharegroupoffsetsrequestgrouptopic, err
		}
		describesharegroupoffsetsrequestgrouptopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return describesharegroupoffsetsrequestgrouptopic, err
		}
		describesharegroupoffsetsrequestgrouptopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return describesharegroupoffsetsrequestgrouptopic, err
		}
		describesharegroupoffsetsrequestgrouptopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return describesharegroupoffsetsrequestgrouptopic, err
		}
		describesharegroupoffsetsrequestgrouptopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describesharegroupoffsetsrequestgrouptopic, err
		}
		describesharegroupoffsetsrequestgrouptopic.rawTaggedFields = &rawTaggedFields
	}

	return describesharegroupoffsetsrequestgrouptopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeShareGroupOffsetsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeShareGroupOffsetsRequest:\n")

	if req.Groups != nil {
		fmt.Fprintf(w, "        Groups:\n")
		for _, groups := range *req.Groups {
			fmt.Fprintf(w, "%s", groups.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeShareGroupOffsetsRequestGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeShareGroupOffsetsRequestGroupTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicName != nil {
		fmt.Fprintf(w, "                TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "                TopicName: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}
