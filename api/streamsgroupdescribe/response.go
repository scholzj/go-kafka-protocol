package streamsgroupdescribe

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type StreamsGroupDescribeResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Groups          *[]StreamsGroupDescribeResponseGroup // Each described group. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroup struct {
	ErrorCode            int16                                      // The describe error, or 0 if there was no error. (versions: 0+)
	ErrorMessage         *string                                    // The top-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	GroupId              *string                                    // The group ID string. (versions: 0+)
	GroupState           *string                                    // The group state string, or the empty string. (versions: 0+)
	GroupEpoch           int32                                      // The group epoch. (versions: 0+)
	AssignmentEpoch      int32                                      // The assignment epoch. (versions: 0+)
	Topology             *StreamsGroupDescribeResponseGroupTopology // The topology metadata currently initialized for the streams application. Can be null in case of a describe error. (versions: 0+, nullable: 0+)
	Members              *[]StreamsGroupDescribeResponseGroupMember // The members. (versions: 0+)
	AuthorizedOperations int32                                      // 32-bit bitfield to represent authorized operations for this group. (versions: 0+)
	rawTaggedFields      *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopology struct {
	Epoch           int32                                                    // The epoch of the currently initialized topology for this group. (versions: 0+)
	Subtopologies   *[]StreamsGroupDescribeResponseGroupTopologySubtopologie // The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopologySubtopologie struct {
	SubtopologyId           *string                                                                        // String to uniquely identify the subtopology. (versions: 0+)
	SourceTopics            *[]string                                                                      // The topics the subtopology reads from. (versions: 0+)
	RepartitionSinkTopics   *[]string                                                                      // The repartition topics the subtopology writes to. (versions: 0+)
	StateChangelogTopics    *[]StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic    // The set of state changelog topics associated with this subtopology. Created automatically. (versions: 0+)
	RepartitionSourceTopics *[]StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic // The set of source topics that are internally created repartition topics. Created automatically. (versions: 0+)
	rawTaggedFields         *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic struct {
	Name              *string                                                                                // The name of the topic. (versions: 0+)
	Partitions        int32                                                                                  // The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics. (versions: 0+)
	ReplicationFactor int16                                                                                  // The replication factor of the topic. Can be 0 if the default replication factor should be used. (versions: 0+)
	TopicConfigs      *[]StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig // Topic-level configurations as key-value pairs. (versions: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig struct {
	Key             *string // key of the config (versions: 0+)
	Value           *string // value of the config (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic struct {
	Name              *string                                                                                   // The name of the topic. (versions: 0+)
	Partitions        int32                                                                                     // The number of partitions in the topic. Can be 0 if no specific number of partitions is enforced. Always 0 for changelog topics. (versions: 0+)
	ReplicationFactor int16                                                                                     // The replication factor of the topic. Can be 0 if the default replication factor should be used. (versions: 0+)
	TopicConfigs      *[]StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig // Topic-level configurations as key-value pairs. (versions: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig struct {
	Key             *string // key of the config (versions: 0+)
	Value           *string // value of the config (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMember struct {
	MemberId         *string                                                  // The member ID. (versions: 0+)
	MemberEpoch      int32                                                    // The member epoch. (versions: 0+)
	InstanceId       *string                                                  // The member instance ID for static membership. (versions: 0+, nullable: 0+)
	RackId           *string                                                  // The rack ID. (versions: 0+, nullable: 0+)
	ClientId         *string                                                  // The client ID. (versions: 0+)
	ClientHost       *string                                                  // The client host. (versions: 0+)
	TopologyEpoch    int32                                                    // The epoch of the topology on the client. (versions: 0+)
	ProcessId        *string                                                  // Identity of the streams instance that may have multiple clients.  (versions: 0+)
	UserEndpoint     *StreamsGroupDescribeResponseGroupMemberUserEndpoint     // User-defined endpoint for Interactive Queries. Null if not defined for this client. (versions: 0+, nullable: 0+)
	ClientTags       *[]StreamsGroupDescribeResponseGroupMemberClientTag      // Used for rack-aware assignment algorithm. (versions: 0+)
	TaskOffsets      *[]StreamsGroupDescribeResponseGroupMemberTaskOffset     // Cumulative changelog offsets for tasks. (versions: 0+)
	TaskEndOffsets   *[]StreamsGroupDescribeResponseGroupMemberTaskEndOffset  // Cumulative changelog end offsets for tasks. (versions: 0+)
	Assignment       *StreamsGroupDescribeResponseGroupMemberAssignment       // The current assignment. (versions: 0+)
	TargetAssignment *StreamsGroupDescribeResponseGroupMemberTargetAssignment // The target assignment. (versions: 0+)
	IsClassic        bool                                                     // True for classic members that have not been upgraded yet. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberUserEndpoint struct {
	Host            *string // host of the endpoint (versions: 0+)
	Port            uint16  // port of the endpoint (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberClientTag struct {
	Key             *string // key of the config (versions: 0+)
	Value           *string // value of the config (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTaskOffset struct {
	SubtopologyId   *string // The subtopology identifier. (versions: 0+)
	Partition       int32   // The partition. (versions: 0+)
	Offset          int64   // The offset. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTaskEndOffset struct {
	SubtopologyId   *string // The subtopology identifier. (versions: 0+)
	Partition       int32   // The partition. (versions: 0+)
	Offset          int64   // The offset. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberAssignment struct {
	ActiveTasks     *[]StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask  // Active tasks for this client. (versions: 0+)
	StandbyTasks    *[]StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask // Standby tasks for this client. (versions: 0+)
	WarmupTasks     *[]StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask  // Warm-up tasks for this client.  (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTargetAssignment struct {
	ActiveTasks     *[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask  // Active tasks for this client. (versions: 0+)
	StandbyTasks    *[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask // Standby tasks for this client. (versions: 0+)
	WarmupTasks     *[]StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask  // Warm-up tasks for this client.  (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask struct {
	SubtopologyId   *string  // The subtopology identifier. (versions: 0+)
	Partitions      *[]int32 // The partitions of the input topics processed by this member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *StreamsGroupDescribeResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Groups (versions: 0+)
	if res.Groups == nil {
		return fmt.Errorf("StreamsGroupDescribeResponse.Groups must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.groupsEncoder, res.Groups); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.groupsEncoder, *res.Groups); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields := []protocol.TaggedField{}
		if res.rawTaggedFields != nil {
			rawTaggedFields = *res.rawTaggedFields
		}
		if err := protocol.WriteRawTaggedFields(w, rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (res *StreamsGroupDescribeResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("StreamsGroupDescribeResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// Groups (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groups, err := protocol.ReadNullableCompactArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = groups
	} else {
		groups, err := protocol.ReadArray(r, res.groupsDecoder)
		if err != nil {
			return err
		}
		res.Groups = &groups
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

func (res *StreamsGroupDescribeResponse) groupsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroup) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.ErrorMessage); err != nil {
			return err
		}
	}

	// GroupId (versions: 0+)
	if value.GroupId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupId); err != nil {
			return err
		}
	}

	// GroupState (versions: 0+)
	if value.GroupState == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroup.GroupState must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.GroupState); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.GroupState); err != nil {
			return err
		}
	}

	// GroupEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.GroupEpoch); err != nil {
		return err
	}

	// AssignmentEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.AssignmentEpoch); err != nil {
		return err
	}

	// Topology (versions: 0+)
	if err := res.topologyEncoder(w, *value.Topology); err != nil {
		return err
	}

	// Members (versions: 0+)
	if value.Members == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroup.Members must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.membersEncoder, value.Members); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.membersEncoder, *value.Members); err != nil {
			return err
		}
	}

	// AuthorizedOperations (versions: 0+)
	if err := protocol.WriteInt32(w, value.AuthorizedOperations); err != nil {
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

func (res *StreamsGroupDescribeResponse) groupsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroup, error) {
	streamsgroupdescriberesponsegroup := StreamsGroupDescribeResponseGroup{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return streamsgroupdescriberesponsegroup, err
	}
	streamsgroupdescriberesponsegroup.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.ErrorMessage = errormessage
	}

	// GroupId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.GroupId = &groupid
	}

	// GroupState (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupstate, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.GroupState = &groupstate
	} else {
		groupstate, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.GroupState = &groupstate
	}

	// GroupEpoch (versions: 0+)
	groupepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroup, err
	}
	streamsgroupdescriberesponsegroup.GroupEpoch = groupepoch

	// AssignmentEpoch (versions: 0+)
	assignmentepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroup, err
	}
	streamsgroupdescriberesponsegroup.AssignmentEpoch = assignmentepoch

	// Topology (versions: 0+)
	topology, err := res.topologyDecoder(r)
	if err != nil {
		return streamsgroupdescriberesponsegroup, err
	}
	streamsgroupdescriberesponsegroup.Topology = &topology

	// Members (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		members, err := protocol.ReadNullableCompactArray(r, res.membersDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.Members = members
	} else {
		members, err := protocol.ReadArray(r, res.membersDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.Members = &members
	}

	// AuthorizedOperations (versions: 0+)
	authorizedoperations, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroup, err
	}
	streamsgroupdescriberesponsegroup.AuthorizedOperations = authorizedoperations

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroup, err
		}
		streamsgroupdescriberesponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroup, nil
}

func (res *StreamsGroupDescribeResponse) topologyEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopology) error {
	// Epoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.Epoch); err != nil {
		return err
	}

	// Subtopologies (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.subtopologiesEncoder, value.Subtopologies); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, res.subtopologiesEncoder, value.Subtopologies); err != nil {
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

func (res *StreamsGroupDescribeResponse) topologyDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopology, error) {
	streamsgroupdescriberesponsegrouptopology := StreamsGroupDescribeResponseGroupTopology{}

	// Epoch (versions: 0+)
	epoch, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegrouptopology, err
	}
	streamsgroupdescriberesponsegrouptopology.Epoch = epoch

	// Subtopologies (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologies, err := protocol.ReadNullableCompactArray(r, res.subtopologiesDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopology, err
		}
		streamsgroupdescriberesponsegrouptopology.Subtopologies = subtopologies
	} else {
		subtopologies, err := protocol.ReadNullableArray(r, res.subtopologiesDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopology, err
		}
		streamsgroupdescriberesponsegrouptopology.Subtopologies = subtopologies
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopology, err
		}
		streamsgroupdescriberesponsegrouptopology.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopology, nil
}

func (res *StreamsGroupDescribeResponse) subtopologiesEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopologySubtopologie) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologie.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// SourceTopics (versions: 0+)
	if value.SourceTopics == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologie.SourceTopics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, value.SourceTopics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *value.SourceTopics); err != nil {
			return err
		}
	}

	// RepartitionSinkTopics (versions: 0+)
	if value.RepartitionSinkTopics == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologie.RepartitionSinkTopics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, value.RepartitionSinkTopics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *value.RepartitionSinkTopics); err != nil {
			return err
		}
	}

	// StateChangelogTopics (versions: 0+)
	if value.StateChangelogTopics == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologie.StateChangelogTopics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.stateChangelogTopicsEncoder, value.StateChangelogTopics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.stateChangelogTopicsEncoder, *value.StateChangelogTopics); err != nil {
			return err
		}
	}

	// RepartitionSourceTopics (versions: 0+)
	if value.RepartitionSourceTopics == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologie.RepartitionSourceTopics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.repartitionSourceTopicsEncoder, value.RepartitionSourceTopics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.repartitionSourceTopicsEncoder, *value.RepartitionSourceTopics); err != nil {
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

func (res *StreamsGroupDescribeResponse) subtopologiesDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopologySubtopologie, error) {
	streamsgroupdescriberesponsegrouptopologysubtopologie := StreamsGroupDescribeResponseGroupTopologySubtopologie{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.SubtopologyId = &subtopologyid
	}

	// SourceTopics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		sourcetopics, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.SourceTopics = sourcetopics
	} else {
		sourcetopics, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.SourceTopics = &sourcetopics
	}

	// RepartitionSinkTopics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		repartitionsinktopics, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.RepartitionSinkTopics = repartitionsinktopics
	} else {
		repartitionsinktopics, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.RepartitionSinkTopics = &repartitionsinktopics
	}

	// StateChangelogTopics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		statechangelogtopics, err := protocol.ReadNullableCompactArray(r, res.stateChangelogTopicsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.StateChangelogTopics = statechangelogtopics
	} else {
		statechangelogtopics, err := protocol.ReadArray(r, res.stateChangelogTopicsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.StateChangelogTopics = &statechangelogtopics
	}

	// RepartitionSourceTopics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		repartitionsourcetopics, err := protocol.ReadNullableCompactArray(r, res.repartitionSourceTopicsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.RepartitionSourceTopics = repartitionsourcetopics
	} else {
		repartitionsourcetopics, err := protocol.ReadArray(r, res.repartitionSourceTopicsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.RepartitionSourceTopics = &repartitionsourcetopics
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologie, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologie.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopologysubtopologie, nil
}

func (res *StreamsGroupDescribeResponse) stateChangelogTopicsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic.Name must not be nil in version %d", res.ApiVersion)
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
	if err := protocol.WriteInt32(w, value.Partitions); err != nil {
		return err
	}

	// ReplicationFactor (versions: 0+)
	if err := protocol.WriteInt16(w, value.ReplicationFactor); err != nil {
		return err
	}

	// TopicConfigs (versions: 0+)
	if value.TopicConfigs == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic.TopicConfigs must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicConfigsEncoder, value.TopicConfigs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicConfigsEncoder, *value.TopicConfigs); err != nil {
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

func (res *StreamsGroupDescribeResponse) stateChangelogTopicsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic, error) {
	streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic := StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.Name = &name
	}

	// Partitions (versions: 0+)
	partitions, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
	}
	streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.Partitions = partitions

	// ReplicationFactor (versions: 0+)
	replicationfactor, err := protocol.ReadInt16(r)
	if err != nil {
		return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
	}
	streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.ReplicationFactor = replicationfactor

	// TopicConfigs (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicconfigs, err := protocol.ReadNullableCompactArray(r, res.topicConfigsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.TopicConfigs = topicconfigs
	} else {
		topicconfigs, err := protocol.ReadArray(r, res.topicConfigsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.TopicConfigs = &topicconfigs
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopic, nil
}

func (res *StreamsGroupDescribeResponse) topicConfigsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig) error {
	// Key (versions: 0+)
	if value.Key == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig.Key must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Key); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Key); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if value.Value == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig.Value must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Value); err != nil {
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

func (res *StreamsGroupDescribeResponse) topicConfigsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig, error) {
	streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig := StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig{}

	// Key (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		key, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig.Key = &key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig.Key = &key
	}

	// Value (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		value, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig.Value = &value
	} else {
		value, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig.Value = &value
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopologysubtopologiestatechangelogtopictopicconfig, nil
}

func (res *StreamsGroupDescribeResponse) repartitionSourceTopicsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic.Name must not be nil in version %d", res.ApiVersion)
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
	if err := protocol.WriteInt32(w, value.Partitions); err != nil {
		return err
	}

	// ReplicationFactor (versions: 0+)
	if err := protocol.WriteInt16(w, value.ReplicationFactor); err != nil {
		return err
	}

	// TopicConfigs (versions: 0+)
	if value.TopicConfigs == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic.TopicConfigs must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigEncoder, value.TopicConfigs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigEncoder, *value.TopicConfigs); err != nil {
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

func (res *StreamsGroupDescribeResponse) repartitionSourceTopicsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic, error) {
	streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic := StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.Name = &name
	}

	// Partitions (versions: 0+)
	partitions, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
	}
	streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.Partitions = partitions

	// ReplicationFactor (versions: 0+)
	replicationfactor, err := protocol.ReadInt16(r)
	if err != nil {
		return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
	}
	streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.ReplicationFactor = replicationfactor

	// TopicConfigs (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicconfigs, err := protocol.ReadNullableCompactArray(r, res.streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.TopicConfigs = topicconfigs
	} else {
		topicconfigs, err := protocol.ReadArray(r, res.streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.TopicConfigs = &topicconfigs
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopic, nil
}

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig) error {
	// Key (versions: 0+)
	if value.Key == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig.Key must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Key); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Key); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if value.Value == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig.Value must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Value); err != nil {
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

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfigDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig, error) {
	streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig := StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig{}

	// Key (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		key, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig.Key = &key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig.Key = &key
	}

	// Value (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		value, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig.Value = &value
	} else {
		value, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig.Value = &value
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, err
		}
		streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegrouptopologysubtopologierepartitionsourcetopictopicconfig, nil
}

func (res *StreamsGroupDescribeResponse) membersEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMember) error {
	// MemberId (versions: 0+)
	if value.MemberId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.MemberId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.MemberId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.MemberId); err != nil {
			return err
		}
	}

	// MemberEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.MemberEpoch); err != nil {
		return err
	}

	// InstanceId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.InstanceId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.InstanceId); err != nil {
			return err
		}
	}

	// RackId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.RackId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.RackId); err != nil {
			return err
		}
	}

	// ClientId (versions: 0+)
	if value.ClientId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.ClientId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ClientId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ClientId); err != nil {
			return err
		}
	}

	// ClientHost (versions: 0+)
	if value.ClientHost == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.ClientHost must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ClientHost); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ClientHost); err != nil {
			return err
		}
	}

	// TopologyEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.TopologyEpoch); err != nil {
		return err
	}

	// ProcessId (versions: 0+)
	if value.ProcessId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.ProcessId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.ProcessId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.ProcessId); err != nil {
			return err
		}
	}

	// UserEndpoint (versions: 0+)
	if err := res.userEndpointEncoder(w, *value.UserEndpoint); err != nil {
		return err
	}

	// ClientTags (versions: 0+)
	if value.ClientTags == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.ClientTags must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.clientTagsEncoder, value.ClientTags); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.clientTagsEncoder, *value.ClientTags); err != nil {
			return err
		}
	}

	// TaskOffsets (versions: 0+)
	if value.TaskOffsets == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.TaskOffsets must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.taskOffsetsEncoder, value.TaskOffsets); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.taskOffsetsEncoder, *value.TaskOffsets); err != nil {
			return err
		}
	}

	// TaskEndOffsets (versions: 0+)
	if value.TaskEndOffsets == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.TaskEndOffsets must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.taskEndOffsetsEncoder, value.TaskEndOffsets); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.taskEndOffsetsEncoder, *value.TaskEndOffsets); err != nil {
			return err
		}
	}

	// Assignment (versions: 0+)
	if value.Assignment == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.Assignment must not be nil in version %d", res.ApiVersion)
	}
	if err := res.assignmentEncoder(w, *value.Assignment); err != nil {
		return err
	}

	// TargetAssignment (versions: 0+)
	if value.TargetAssignment == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMember.TargetAssignment must not be nil in version %d", res.ApiVersion)
	}
	if err := res.targetAssignmentEncoder(w, *value.TargetAssignment); err != nil {
		return err
	}

	// IsClassic (versions: 0+)
	if err := protocol.WriteBool(w, value.IsClassic); err != nil {
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

func (res *StreamsGroupDescribeResponse) membersDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMember, error) {
	streamsgroupdescriberesponsegroupmember := StreamsGroupDescribeResponseGroupMember{}

	// MemberId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.MemberId = &memberid
	}

	// MemberEpoch (versions: 0+)
	memberepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.MemberEpoch = memberepoch

	// InstanceId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		instanceid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.InstanceId = instanceid
	} else {
		instanceid, err := protocol.ReadNullableString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.InstanceId = instanceid
	}

	// RackId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		rackid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.RackId = rackid
	} else {
		rackid, err := protocol.ReadNullableString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.RackId = rackid
	}

	// ClientId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clientid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientId = &clientid
	} else {
		clientid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientId = &clientid
	}

	// ClientHost (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clienthost, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientHost = &clienthost
	} else {
		clienthost, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientHost = &clienthost
	}

	// TopologyEpoch (versions: 0+)
	topologyepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.TopologyEpoch = topologyepoch

	// ProcessId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		processid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ProcessId = &processid
	} else {
		processid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ProcessId = &processid
	}

	// UserEndpoint (versions: 0+)
	userendpoint, err := res.userEndpointDecoder(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.UserEndpoint = &userendpoint

	// ClientTags (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clienttags, err := protocol.ReadNullableCompactArray(r, res.clientTagsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientTags = clienttags
	} else {
		clienttags, err := protocol.ReadArray(r, res.clientTagsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.ClientTags = &clienttags
	}

	// TaskOffsets (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		taskoffsets, err := protocol.ReadNullableCompactArray(r, res.taskOffsetsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.TaskOffsets = taskoffsets
	} else {
		taskoffsets, err := protocol.ReadArray(r, res.taskOffsetsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.TaskOffsets = &taskoffsets
	}

	// TaskEndOffsets (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		taskendoffsets, err := protocol.ReadNullableCompactArray(r, res.taskEndOffsetsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.TaskEndOffsets = taskendoffsets
	} else {
		taskendoffsets, err := protocol.ReadArray(r, res.taskEndOffsetsDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.TaskEndOffsets = &taskendoffsets
	}

	// Assignment (versions: 0+)
	assignment, err := res.assignmentDecoder(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.Assignment = &assignment

	// TargetAssignment (versions: 0+)
	targetassignment, err := res.targetAssignmentDecoder(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.TargetAssignment = &targetassignment

	// IsClassic (versions: 0+)
	isclassic, err := protocol.ReadBool(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmember, err
	}
	streamsgroupdescriberesponsegroupmember.IsClassic = isclassic

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmember, err
		}
		streamsgroupdescriberesponsegroupmember.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmember, nil
}

func (res *StreamsGroupDescribeResponse) userEndpointEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberUserEndpoint) error {
	// Host (versions: 0+)
	if value.Host == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberUserEndpoint.Host must not be nil in version %d", res.ApiVersion)
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

	// Port (versions: 0+)
	if err := protocol.WriteUint16(w, value.Port); err != nil {
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

func (res *StreamsGroupDescribeResponse) userEndpointDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberUserEndpoint, error) {
	streamsgroupdescriberesponsegroupmemberuserendpoint := StreamsGroupDescribeResponseGroupMemberUserEndpoint{}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberuserendpoint, err
		}
		streamsgroupdescriberesponsegroupmemberuserendpoint.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberuserendpoint, err
		}
		streamsgroupdescriberesponsegroupmemberuserendpoint.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadUInt16(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmemberuserendpoint, err
	}
	streamsgroupdescriberesponsegroupmemberuserendpoint.Port = port

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberuserendpoint, err
		}
		streamsgroupdescriberesponsegroupmemberuserendpoint.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberuserendpoint, nil
}

func (res *StreamsGroupDescribeResponse) clientTagsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberClientTag) error {
	// Key (versions: 0+)
	if value.Key == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberClientTag.Key must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Key); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Key); err != nil {
			return err
		}
	}

	// Value (versions: 0+)
	if value.Value == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberClientTag.Value must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Value); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Value); err != nil {
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

func (res *StreamsGroupDescribeResponse) clientTagsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberClientTag, error) {
	streamsgroupdescriberesponsegroupmemberclienttag := StreamsGroupDescribeResponseGroupMemberClientTag{}

	// Key (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		key, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberclienttag, err
		}
		streamsgroupdescriberesponsegroupmemberclienttag.Key = &key
	} else {
		key, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberclienttag, err
		}
		streamsgroupdescriberesponsegroupmemberclienttag.Key = &key
	}

	// Value (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		value, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberclienttag, err
		}
		streamsgroupdescriberesponsegroupmemberclienttag.Value = &value
	} else {
		value, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberclienttag, err
		}
		streamsgroupdescriberesponsegroupmemberclienttag.Value = &value
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberclienttag, err
		}
		streamsgroupdescriberesponsegroupmemberclienttag.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberclienttag, nil
}

func (res *StreamsGroupDescribeResponse) taskOffsetsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTaskOffset) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTaskOffset.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// Offset (versions: 0+)
	if err := protocol.WriteInt64(w, value.Offset); err != nil {
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

func (res *StreamsGroupDescribeResponse) taskOffsetsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTaskOffset, error) {
	streamsgroupdescriberesponsegroupmembertaskoffset := StreamsGroupDescribeResponseGroupMemberTaskOffset{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskoffset.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskoffset.SubtopologyId = &subtopologyid
	}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmembertaskoffset, err
	}
	streamsgroupdescriberesponsegroupmembertaskoffset.Partition = partition

	// Offset (versions: 0+)
	offset, err := protocol.ReadInt64(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmembertaskoffset, err
	}
	streamsgroupdescriberesponsegroupmembertaskoffset.Offset = offset

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskoffset.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertaskoffset, nil
}

func (res *StreamsGroupDescribeResponse) taskEndOffsetsEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTaskEndOffset) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTaskEndOffset.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partition (versions: 0+)
	if err := protocol.WriteInt32(w, value.Partition); err != nil {
		return err
	}

	// Offset (versions: 0+)
	if err := protocol.WriteInt64(w, value.Offset); err != nil {
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

func (res *StreamsGroupDescribeResponse) taskEndOffsetsDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTaskEndOffset, error) {
	streamsgroupdescriberesponsegroupmembertaskendoffset := StreamsGroupDescribeResponseGroupMemberTaskEndOffset{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskendoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskendoffset.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskendoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskendoffset.SubtopologyId = &subtopologyid
	}

	// Partition (versions: 0+)
	partition, err := protocol.ReadInt32(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmembertaskendoffset, err
	}
	streamsgroupdescriberesponsegroupmembertaskendoffset.Partition = partition

	// Offset (versions: 0+)
	offset, err := protocol.ReadInt64(r)
	if err != nil {
		return streamsgroupdescriberesponsegroupmembertaskendoffset, err
	}
	streamsgroupdescriberesponsegroupmembertaskendoffset.Offset = offset

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertaskendoffset, err
		}
		streamsgroupdescriberesponsegroupmembertaskendoffset.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertaskendoffset, nil
}

func (res *StreamsGroupDescribeResponse) assignmentEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberAssignment) error {
	// ActiveTasks (versions: 0+)
	if value.ActiveTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignment.ActiveTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.activeTasksEncoder, value.ActiveTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.activeTasksEncoder, *value.ActiveTasks); err != nil {
			return err
		}
	}

	// StandbyTasks (versions: 0+)
	if value.StandbyTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignment.StandbyTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.standbyTasksEncoder, value.StandbyTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.standbyTasksEncoder, *value.StandbyTasks); err != nil {
			return err
		}
	}

	// WarmupTasks (versions: 0+)
	if value.WarmupTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignment.WarmupTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.warmupTasksEncoder, value.WarmupTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.warmupTasksEncoder, *value.WarmupTasks); err != nil {
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

func (res *StreamsGroupDescribeResponse) assignmentDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberAssignment, error) {
	streamsgroupdescriberesponsegroupmemberassignment := StreamsGroupDescribeResponseGroupMemberAssignment{}

	// ActiveTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		activetasks, err := protocol.ReadNullableCompactArray(r, res.activeTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.ActiveTasks = activetasks
	} else {
		activetasks, err := protocol.ReadArray(r, res.activeTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.ActiveTasks = &activetasks
	}

	// StandbyTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		standbytasks, err := protocol.ReadNullableCompactArray(r, res.standbyTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.StandbyTasks = standbytasks
	} else {
		standbytasks, err := protocol.ReadArray(r, res.standbyTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.StandbyTasks = &standbytasks
	}

	// WarmupTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		warmuptasks, err := protocol.ReadNullableCompactArray(r, res.warmupTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.WarmupTasks = warmuptasks
	} else {
		warmuptasks, err := protocol.ReadArray(r, res.warmupTasksDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.WarmupTasks = &warmuptasks
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignment, err
		}
		streamsgroupdescriberesponsegroupmemberassignment.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberassignment, nil
}

func (res *StreamsGroupDescribeResponse) activeTasksEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) activeTasksDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask, error) {
	streamsgroupdescriberesponsegroupmemberassignmentactivetask := StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentactivetask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentactivetask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentactivetask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentactivetask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentactivetask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberassignmentactivetask, nil
}

func (res *StreamsGroupDescribeResponse) standbyTasksEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) standbyTasksDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask, error) {
	streamsgroupdescriberesponsegroupmemberassignmentstandbytask := StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentstandbytask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentstandbytask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentstandbytask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentstandbytask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentstandbytask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberassignmentstandbytask, nil
}

func (res *StreamsGroupDescribeResponse) warmupTasksEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) warmupTasksDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask, error) {
	streamsgroupdescriberesponsegroupmemberassignmentwarmuptask := StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentwarmuptask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentwarmuptask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentwarmuptask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentwarmuptask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmemberassignmentwarmuptask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmemberassignmentwarmuptask, nil
}

func (res *StreamsGroupDescribeResponse) targetAssignmentEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTargetAssignment) error {
	// ActiveTasks (versions: 0+)
	if value.ActiveTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignment.ActiveTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskEncoder, value.ActiveTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskEncoder, *value.ActiveTasks); err != nil {
			return err
		}
	}

	// StandbyTasks (versions: 0+)
	if value.StandbyTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignment.StandbyTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskEncoder, value.StandbyTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskEncoder, *value.StandbyTasks); err != nil {
			return err
		}
	}

	// WarmupTasks (versions: 0+)
	if value.WarmupTasks == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignment.WarmupTasks must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskEncoder, value.WarmupTasks); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskEncoder, *value.WarmupTasks); err != nil {
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

func (res *StreamsGroupDescribeResponse) targetAssignmentDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTargetAssignment, error) {
	streamsgroupdescriberesponsegroupmembertargetassignment := StreamsGroupDescribeResponseGroupMemberTargetAssignment{}

	// ActiveTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		activetasks, err := protocol.ReadNullableCompactArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.ActiveTasks = activetasks
	} else {
		activetasks, err := protocol.ReadArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.ActiveTasks = &activetasks
	}

	// StandbyTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		standbytasks, err := protocol.ReadNullableCompactArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.StandbyTasks = standbytasks
	} else {
		standbytasks, err := protocol.ReadArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.StandbyTasks = &standbytasks
	}

	// WarmupTasks (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		warmuptasks, err := protocol.ReadNullableCompactArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.WarmupTasks = warmuptasks
	} else {
		warmuptasks, err := protocol.ReadArray(r, res.streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskDecoder)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.WarmupTasks = &warmuptasks
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignment, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignment.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertargetassignment, nil
}

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTaskDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask, error) {
	streamsgroupdescriberesponsegroupmembertargetassignmentactivetask := StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentactivetask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentactivetask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentactivetask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentactivetask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentactivetask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertargetassignmentactivetask, nil
}

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTaskDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask, error) {
	streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask := StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertargetassignmentstandbytask, nil
}

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskEncoder(w io.Writer, value StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask) error {
	// SubtopologyId (versions: 0+)
	if value.SubtopologyId == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask.SubtopologyId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.SubtopologyId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.SubtopologyId); err != nil {
			return err
		}
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (res *StreamsGroupDescribeResponse) streamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTaskDecoder(r io.Reader) (StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask, error) {
	streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask := StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask{}

	// SubtopologyId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subtopologyid, err := protocol.ReadCompactString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask.SubtopologyId = &subtopologyid
	} else {
		subtopologyid, err := protocol.ReadString(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask.SubtopologyId = &subtopologyid
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, err
		}
		streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask.rawTaggedFields = &rawTaggedFields
	}

	return streamsgroupdescriberesponsegroupmembertargetassignmentwarmuptask, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *StreamsGroupDescribeResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- StreamsGroupDescribeResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	if res.Groups != nil {
		fmt.Fprintf(w, "        Groups:\n")
		for _, groups := range *res.Groups {
			fmt.Fprintf(w, "%s", groups.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroup) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.ErrorMessage != nil {
		fmt.Fprintf(w, "            ErrorMessage: %v\n", *value.ErrorMessage)
	} else {
		fmt.Fprintf(w, "            ErrorMessage: nil\n")
	}

	if value.GroupId != nil {
		fmt.Fprintf(w, "            GroupId: %v\n", *value.GroupId)
	} else {
		fmt.Fprintf(w, "            GroupId: nil\n")
	}

	if value.GroupState != nil {
		fmt.Fprintf(w, "            GroupState: %v\n", *value.GroupState)
	} else {
		fmt.Fprintf(w, "            GroupState: nil\n")
	}

	fmt.Fprintf(w, "            GroupEpoch: %v\n", value.GroupEpoch)
	fmt.Fprintf(w, "            AssignmentEpoch: %v\n", value.AssignmentEpoch)

	fmt.Fprintf(w, "            Topology:\n")
	if value.Topology != nil {
		fmt.Fprintf(w, "%s", value.Topology.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                nil\n")
	}

	if value.Members != nil {
		fmt.Fprintf(w, "            Members:\n")
		for _, members := range *value.Members {
			fmt.Fprintf(w, "%s", members.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Members: nil\n")
	}

	fmt.Fprintf(w, "            AuthorizedOperations: %v\n", value.AuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopology) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                Epoch: %v\n", value.Epoch)

	if value.Subtopologies != nil {
		fmt.Fprintf(w, "                Subtopologies:\n")
		for _, subtopologies := range *value.Subtopologies {
			fmt.Fprintf(w, "%s", subtopologies.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Subtopologies: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopologySubtopologie) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                    SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                    SubtopologyId: nil\n")
	}

	if value.SourceTopics != nil {
		fmt.Fprintf(w, "                    SourceTopics: %v\n", *value.SourceTopics)
	} else {
		fmt.Fprintf(w, "                    SourceTopics: nil\n")
	}

	if value.RepartitionSinkTopics != nil {
		fmt.Fprintf(w, "                    RepartitionSinkTopics: %v\n", *value.RepartitionSinkTopics)
	} else {
		fmt.Fprintf(w, "                    RepartitionSinkTopics: nil\n")
	}

	if value.StateChangelogTopics != nil {
		fmt.Fprintf(w, "                    StateChangelogTopics:\n")
		for _, statechangelogtopics := range *value.StateChangelogTopics {
			fmt.Fprintf(w, "%s", statechangelogtopics.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    StateChangelogTopics: nil\n")
	}

	if value.RepartitionSourceTopics != nil {
		fmt.Fprintf(w, "                    RepartitionSourceTopics:\n")
		for _, repartitionsourcetopics := range *value.RepartitionSourceTopics {
			fmt.Fprintf(w, "%s", repartitionsourcetopics.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    RepartitionSourceTopics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                        Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                        Name: nil\n")
	}

	fmt.Fprintf(w, "                        Partitions: %v\n", value.Partitions)
	fmt.Fprintf(w, "                        ReplicationFactor: %v\n", value.ReplicationFactor)

	if value.TopicConfigs != nil {
		fmt.Fprintf(w, "                        TopicConfigs:\n")
		for _, topicconfigs := range *value.TopicConfigs {
			fmt.Fprintf(w, "%s", topicconfigs.PrettyPrint())
			fmt.Fprintf(w, "                            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                        TopicConfigs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopologySubtopologieStateChangelogTopicTopicConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "                            Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "                            Key: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                            Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                            Value: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                        Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                        Name: nil\n")
	}

	fmt.Fprintf(w, "                        Partitions: %v\n", value.Partitions)
	fmt.Fprintf(w, "                        ReplicationFactor: %v\n", value.ReplicationFactor)

	if value.TopicConfigs != nil {
		fmt.Fprintf(w, "                        TopicConfigs:\n")
		for _, topicconfigs := range *value.TopicConfigs {
			fmt.Fprintf(w, "%s", topicconfigs.PrettyPrint())
			fmt.Fprintf(w, "                            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                        TopicConfigs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupTopologySubtopologieRepartitionSourceTopicTopicConfig) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "                            Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "                            Key: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                            Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                            Value: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMember) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.MemberId != nil {
		fmt.Fprintf(w, "                MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "                MemberId: nil\n")
	}

	fmt.Fprintf(w, "                MemberEpoch: %v\n", value.MemberEpoch)

	if value.InstanceId != nil {
		fmt.Fprintf(w, "                InstanceId: %v\n", *value.InstanceId)
	} else {
		fmt.Fprintf(w, "                InstanceId: nil\n")
	}

	if value.RackId != nil {
		fmt.Fprintf(w, "                RackId: %v\n", *value.RackId)
	} else {
		fmt.Fprintf(w, "                RackId: nil\n")
	}

	if value.ClientId != nil {
		fmt.Fprintf(w, "                ClientId: %v\n", *value.ClientId)
	} else {
		fmt.Fprintf(w, "                ClientId: nil\n")
	}

	if value.ClientHost != nil {
		fmt.Fprintf(w, "                ClientHost: %v\n", *value.ClientHost)
	} else {
		fmt.Fprintf(w, "                ClientHost: nil\n")
	}

	fmt.Fprintf(w, "                TopologyEpoch: %v\n", value.TopologyEpoch)

	if value.ProcessId != nil {
		fmt.Fprintf(w, "                ProcessId: %v\n", *value.ProcessId)
	} else {
		fmt.Fprintf(w, "                ProcessId: nil\n")
	}

	fmt.Fprintf(w, "                UserEndpoint:\n")
	if value.UserEndpoint != nil {
		fmt.Fprintf(w, "%s", value.UserEndpoint.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	if value.ClientTags != nil {
		fmt.Fprintf(w, "                ClientTags:\n")
		for _, clienttags := range *value.ClientTags {
			fmt.Fprintf(w, "%s", clienttags.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                ClientTags: nil\n")
	}

	if value.TaskOffsets != nil {
		fmt.Fprintf(w, "                TaskOffsets:\n")
		for _, taskoffsets := range *value.TaskOffsets {
			fmt.Fprintf(w, "%s", taskoffsets.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                TaskOffsets: nil\n")
	}

	if value.TaskEndOffsets != nil {
		fmt.Fprintf(w, "                TaskEndOffsets:\n")
		for _, taskendoffsets := range *value.TaskEndOffsets {
			fmt.Fprintf(w, "%s", taskendoffsets.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                TaskEndOffsets: nil\n")
	}

	fmt.Fprintf(w, "                Assignment:\n")
	if value.Assignment != nil {
		fmt.Fprintf(w, "%s", value.Assignment.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	fmt.Fprintf(w, "                TargetAssignment:\n")
	if value.TargetAssignment != nil {
		fmt.Fprintf(w, "%s", value.TargetAssignment.PrettyPrint())
	} else {
		fmt.Fprintf(w, "                    nil\n")
	}

	fmt.Fprintf(w, "                IsClassic: %v\n", value.IsClassic)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberUserEndpoint) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Host != nil {
		fmt.Fprintf(w, "                    Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "                    Host: nil\n")
	}

	fmt.Fprintf(w, "                    Port: %v\n", value.Port)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberClientTag) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Key != nil {
		fmt.Fprintf(w, "                    Key: %v\n", *value.Key)
	} else {
		fmt.Fprintf(w, "                    Key: nil\n")
	}

	if value.Value != nil {
		fmt.Fprintf(w, "                    Value: %v\n", *value.Value)
	} else {
		fmt.Fprintf(w, "                    Value: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTaskOffset) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                    SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                    SubtopologyId: nil\n")
	}

	fmt.Fprintf(w, "                    Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                    Offset: %v\n", value.Offset)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTaskEndOffset) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                    SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                    SubtopologyId: nil\n")
	}

	fmt.Fprintf(w, "                    Partition: %v\n", value.Partition)
	fmt.Fprintf(w, "                    Offset: %v\n", value.Offset)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.ActiveTasks != nil {
		fmt.Fprintf(w, "                    ActiveTasks:\n")
		for _, activetasks := range *value.ActiveTasks {
			fmt.Fprintf(w, "%s", activetasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    ActiveTasks: nil\n")
	}

	if value.StandbyTasks != nil {
		fmt.Fprintf(w, "                    StandbyTasks:\n")
		for _, standbytasks := range *value.StandbyTasks {
			fmt.Fprintf(w, "%s", standbytasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    StandbyTasks: nil\n")
	}

	if value.WarmupTasks != nil {
		fmt.Fprintf(w, "                    WarmupTasks:\n")
		for _, warmuptasks := range *value.WarmupTasks {
			fmt.Fprintf(w, "%s", warmuptasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    WarmupTasks: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberAssignmentActiveTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberAssignmentStandbyTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberAssignmentWarmupTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTargetAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.ActiveTasks != nil {
		fmt.Fprintf(w, "                    ActiveTasks:\n")
		for _, activetasks := range *value.ActiveTasks {
			fmt.Fprintf(w, "%s", activetasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    ActiveTasks: nil\n")
	}

	if value.StandbyTasks != nil {
		fmt.Fprintf(w, "                    StandbyTasks:\n")
		for _, standbytasks := range *value.StandbyTasks {
			fmt.Fprintf(w, "%s", standbytasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    StandbyTasks: nil\n")
	}

	if value.WarmupTasks != nil {
		fmt.Fprintf(w, "                    WarmupTasks:\n")
		for _, warmuptasks := range *value.WarmupTasks {
			fmt.Fprintf(w, "%s", warmuptasks.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    WarmupTasks: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTargetAssignmentActiveTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTargetAssignmentStandbyTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *StreamsGroupDescribeResponseGroupMemberTargetAssignmentWarmupTask) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.SubtopologyId != nil {
		fmt.Fprintf(w, "                        SubtopologyId: %v\n", *value.SubtopologyId)
	} else {
		fmt.Fprintf(w, "                        SubtopologyId: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}
