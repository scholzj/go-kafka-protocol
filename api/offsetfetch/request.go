package offsetfetch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type OffsetFetchRequest struct {
	ApiVersion      int16
	GroupId         *string                    // The group to fetch offsets for. (versions: 0-7)
	Topics          *[]OffsetFetchRequestTopic // Each topic we would like to fetch offsets for, or null to fetch offsets for all topics. (versions: 0-7, nullable: 2-7)
	Groups          *[]OffsetFetchRequestGroup // Each group we would like to fetch offsets for. (versions: 8+)
	RequireStable   bool                       // Whether broker should hold on returning unstable offsets but set a retriable error code for the partitions. (versions: 7+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchRequestTopic struct {
	Name             *string  // The topic name. (versions: 0-7)
	PartitionIndexes *[]int32 // The partition indexes we would like to fetch offsets for. (versions: 0-7)
	rawTaggedFields  *[]protocol.TaggedField
}

type OffsetFetchRequestGroup struct {
	GroupId         *string                         // The group ID. (versions: 8+)
	MemberId        *string                         // The member id. (versions: 9+, nullable: 9+)
	MemberEpoch     int32                           // The member epoch if using the new consumer protocol (KIP-848). (versions: 9+)
	Topics          *[]OffsetFetchRequestGroupTopic // Each topic we would like to fetch offsets for, or null to fetch offsets for all topics. (versions: 8+, nullable: 8+)
	rawTaggedFields *[]protocol.TaggedField
}

type OffsetFetchRequestGroupTopic struct {
	Name             *string   // The topic name. (versions: 8-9)
	TopicId          uuid.UUID // The topic ID. (versions: 10+)
	PartitionIndexes *[]int32  // The partition indexes we would like to fetch offsets for. (versions: 8+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 6
}

func (req *OffsetFetchRequest) Write(w io.Writer) error {
	// GroupId (versions: 0-7)
	if req.ApiVersion <= 7 {
		if req.GroupId == nil {
			return fmt.Errorf("OffsetFetchRequest.GroupId must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.GroupId); err != nil {
				return err
			}
		}
	}

	// Topics (versions: 0-7)
	if req.ApiVersion <= 7 {
		if req.ApiVersion < 2 && req.Topics == nil {
			return fmt.Errorf("OffsetFetchRequest.Topics must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableArray(w, req.topicsEncoder, req.Topics); err != nil {
				return err
			}
		}
	}

	// Groups (versions: 8+)
	if req.ApiVersion >= 8 {
		if req.Groups == nil {
			return fmt.Errorf("OffsetFetchRequest.Groups must not be nil in version %d", req.ApiVersion)
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
	}

	// RequireStable (versions: 7+)
	if req.ApiVersion >= 7 {
		if err := protocol.WriteBool(w, req.RequireStable); err != nil {
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
func (req *OffsetFetchRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("OffsetFetchRequest.Read: request or its body is nil")
	}

	*req = OffsetFetchRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupId (versions: 0-7)
	if req.ApiVersion <= 7 {
		if isRequestFlexible(req.ApiVersion) {
			groupid, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.GroupId = &groupid
		} else {
			groupid, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.GroupId = &groupid
		}
	}

	// Topics (versions: 0-7)
	if req.ApiVersion <= 7 {
		if isRequestFlexible(req.ApiVersion) {
			if req.ApiVersion >= 2 {
				topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
				if err != nil {
					return err
				}
				req.Topics = topics
			} else {
				topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
				if err != nil {
					return err
				}
				req.Topics = &topics
			}
		} else {
			if req.ApiVersion >= 2 {
				topics, err := protocol.ReadNullableArray(r, req.topicsDecoder)
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
		}
	}

	// Groups (versions: 8+)
	if req.ApiVersion >= 8 {
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
	}

	// RequireStable (versions: 7+)
	if req.ApiVersion >= 7 {
		requirestable, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.RequireStable = requirestable
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

func (req *OffsetFetchRequest) topicsEncoder(w io.Writer, value OffsetFetchRequestTopic) error {
	// Name (versions: 0-7)
	if req.ApiVersion <= 7 {
		if value.Name == nil {
			return fmt.Errorf("OffsetFetchRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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
	}

	// PartitionIndexes (versions: 0-7)
	if req.ApiVersion <= 7 {
		if value.PartitionIndexes == nil {
			return fmt.Errorf("OffsetFetchRequestTopic.PartitionIndexes must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.PartitionIndexes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.PartitionIndexes); err != nil {
				return err
			}
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

func (req *OffsetFetchRequest) topicsDecoder(r io.Reader) (OffsetFetchRequestTopic, error) {
	offsetfetchrequesttopic := OffsetFetchRequestTopic{}

	// Name (versions: 0-7)
	if req.ApiVersion <= 7 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchrequesttopic, err
			}
			offsetfetchrequesttopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchrequesttopic, err
			}
			offsetfetchrequesttopic.Name = &name
		}
	}

	// PartitionIndexes (versions: 0-7)
	if req.ApiVersion <= 7 {
		if isRequestFlexible(req.ApiVersion) {
			partitionindexes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return offsetfetchrequesttopic, err
			}
			offsetfetchrequesttopic.PartitionIndexes = &partitionindexes
		} else {
			partitionindexes, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return offsetfetchrequesttopic, err
			}
			offsetfetchrequesttopic.PartitionIndexes = &partitionindexes
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchrequesttopic, err
		}
		offsetfetchrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchrequesttopic, nil
}

func (req *OffsetFetchRequest) groupsEncoder(w io.Writer, value OffsetFetchRequestGroup) error {
	// GroupId (versions: 8+)
	if req.ApiVersion >= 8 {
		if value.GroupId == nil {
			return fmt.Errorf("OffsetFetchRequestGroup.GroupId must not be nil in version %d", req.ApiVersion)
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
	}

	// MemberId (versions: 9+)
	if req.ApiVersion >= 9 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, value.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, value.MemberId); err != nil {
				return err
			}
		}
	}

	// MemberEpoch (versions: 9+)
	if req.ApiVersion >= 9 {
		if err := protocol.WriteInt32(w, value.MemberEpoch); err != nil {
			return err
		}
	}

	// Topics (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.offsetFetchRequestGroupTopicEncoder, value.Topics); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableArray(w, req.offsetFetchRequestGroupTopicEncoder, value.Topics); err != nil {
				return err
			}
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

func (req *OffsetFetchRequest) groupsDecoder(r io.Reader) (OffsetFetchRequestGroup, error) {
	offsetfetchrequestgroup := OffsetFetchRequestGroup{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	offsetfetchrequestgroup.MemberEpoch = -1

	// GroupId (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			groupid, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.GroupId = &groupid
		} else {
			groupid, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.GroupId = &groupid
		}
	}

	// MemberId (versions: 9+)
	if req.ApiVersion >= 9 {
		if isRequestFlexible(req.ApiVersion) {
			memberid, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.MemberId = memberid
		} else {
			memberid, err := protocol.ReadNullableString(r)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.MemberId = memberid
		}
	}

	// MemberEpoch (versions: 9+)
	if req.ApiVersion >= 9 {
		memberepoch, err := protocol.ReadInt32(r)
		if err != nil {
			return offsetfetchrequestgroup, err
		}
		offsetfetchrequestgroup.MemberEpoch = memberepoch
	}

	// Topics (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			topics, err := protocol.ReadNullableCompactArray(r, req.offsetFetchRequestGroupTopicDecoder)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.Topics = topics
		} else {
			topics, err := protocol.ReadNullableArray(r, req.offsetFetchRequestGroupTopicDecoder)
			if err != nil {
				return offsetfetchrequestgroup, err
			}
			offsetfetchrequestgroup.Topics = topics
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchrequestgroup, err
		}
		offsetfetchrequestgroup.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchrequestgroup, nil
}

func (req *OffsetFetchRequest) offsetFetchRequestGroupTopicEncoder(w io.Writer, value OffsetFetchRequestGroupTopic) error {
	// Name (versions: 8-9)
	if req.ApiVersion >= 8 && req.ApiVersion <= 9 {
		if value.Name == nil {
			return fmt.Errorf("OffsetFetchRequestGroupTopic.Name must not be nil in version %d", req.ApiVersion)
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
	}

	// TopicId (versions: 10+)
	if req.ApiVersion >= 10 {
		if err := protocol.WriteUUID(w, value.TopicId); err != nil {
			return err
		}
	}

	// PartitionIndexes (versions: 8+)
	if req.ApiVersion >= 8 {
		if value.PartitionIndexes == nil {
			return fmt.Errorf("OffsetFetchRequestGroupTopic.PartitionIndexes must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.PartitionIndexes); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.PartitionIndexes); err != nil {
				return err
			}
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

func (req *OffsetFetchRequest) offsetFetchRequestGroupTopicDecoder(r io.Reader) (OffsetFetchRequestGroupTopic, error) {
	offsetfetchrequestgrouptopic := OffsetFetchRequestGroupTopic{}

	// Name (versions: 8-9)
	if req.ApiVersion >= 8 && req.ApiVersion <= 9 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return offsetfetchrequestgrouptopic, err
			}
			offsetfetchrequestgrouptopic.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return offsetfetchrequestgrouptopic, err
			}
			offsetfetchrequestgrouptopic.Name = &name
		}
	}

	// TopicId (versions: 10+)
	if req.ApiVersion >= 10 {
		topicid, err := protocol.ReadUUID(r)
		if err != nil {
			return offsetfetchrequestgrouptopic, err
		}
		offsetfetchrequestgrouptopic.TopicId = topicid
	}

	// PartitionIndexes (versions: 8+)
	if req.ApiVersion >= 8 {
		if isRequestFlexible(req.ApiVersion) {
			partitionindexes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return offsetfetchrequestgrouptopic, err
			}
			offsetfetchrequestgrouptopic.PartitionIndexes = &partitionindexes
		} else {
			partitionindexes, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return offsetfetchrequestgrouptopic, err
			}
			offsetfetchrequestgrouptopic.PartitionIndexes = &partitionindexes
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return offsetfetchrequestgrouptopic, err
		}
		offsetfetchrequestgrouptopic.rawTaggedFields = &rawTaggedFields
	}

	return offsetfetchrequestgrouptopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *OffsetFetchRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> OffsetFetchRequest:\n")

	if req.GroupId != nil {
		fmt.Fprintf(w, "        GroupId: %v\n", *req.GroupId)
	} else {
		fmt.Fprintf(w, "        GroupId: nil\n")
	}

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	if req.Groups != nil {
		fmt.Fprintf(w, "        Groups:\n")
		for _, groups := range *req.Groups {
			fmt.Fprintf(w, "%s", groups.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	fmt.Fprintf(w, "        RequireStable: %v\n", req.RequireStable)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetFetchRequestTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.PartitionIndexes != nil {
		fmt.Fprintf(w, "            PartitionIndexes: %v\n", *value.PartitionIndexes)
	} else {
		fmt.Fprintf(w, "            PartitionIndexes: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *OffsetFetchRequestGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.MemberId != nil {
		fmt.Fprintf(w, "            MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "            MemberId: nil\n")
	}

	fmt.Fprintf(w, "            MemberEpoch: %v\n", value.MemberEpoch)

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
func (value *OffsetFetchRequestGroupTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	fmt.Fprintf(w, "                TopicId: %v\n", value.TopicId)

	if value.PartitionIndexes != nil {
		fmt.Fprintf(w, "                PartitionIndexes: %v\n", *value.PartitionIndexes)
	} else {
		fmt.Fprintf(w, "                PartitionIndexes: nil\n")
	}

	return w.String()
}
