package consumergroupdescribe

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ConsumerGroupDescribeResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                 // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	Groups          *[]ConsumerGroupDescribeResponseGroup // Each described group. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroup struct {
	ErrorCode            int16                                       // The describe error, or 0 if there was no error. (versions: 0+)
	ErrorMessage         *string                                     // The top-level error message, or null if there was no error. (versions: 0+, nullable: 0+)
	GroupId              *string                                     // The group ID string. (versions: 0+)
	GroupState           *string                                     // The group state string, or the empty string. (versions: 0+)
	GroupEpoch           int32                                       // The group epoch. (versions: 0+)
	AssignmentEpoch      int32                                       // The assignment epoch. (versions: 0+)
	AssignorName         *string                                     // The selected assignor. (versions: 0+)
	Members              *[]ConsumerGroupDescribeResponseGroupMember // The members. (versions: 0+)
	AuthorizedOperations int32                                       // 32-bit bitfield to represent authorized operations for this group. (versions: 0+)
	rawTaggedFields      *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroupMember struct {
	MemberId             *string                                                   // The member ID. (versions: 0+)
	InstanceId           *string                                                   // The member instance ID. (versions: 0+, nullable: 0+)
	RackId               *string                                                   // The member rack ID. (versions: 0+, nullable: 0+)
	MemberEpoch          int32                                                     // The current member epoch. (versions: 0+)
	ClientId             *string                                                   // The client ID. (versions: 0+)
	ClientHost           *string                                                   // The client host. (versions: 0+)
	SubscribedTopicNames *[]string                                                 // The subscribed topic names. (versions: 0+)
	SubscribedTopicRegex *string                                                   // the subscribed topic regex otherwise or null of not provided. (versions: 0+, nullable: 0+)
	Assignment           *ConsumerGroupDescribeResponseGroupMemberAssignment       // The current assignment. (versions: 0+)
	TargetAssignment     *ConsumerGroupDescribeResponseGroupMemberTargetAssignment // The target assignment. (versions: 0+)
	MemberType           int8                                                      // -1 for unknown. 0 for classic member. +1 for consumer member. (versions: 1+)
	rawTaggedFields      *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroupMemberAssignment struct {
	TopicPartitions *[]ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition // The assigned topic-partitions to the member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition struct {
	TopicId         uuid.UUID // The topic ID. (versions: 0+)
	TopicName       *string   // The topic name. (versions: 0+)
	Partitions      *[]int32  // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroupMemberTargetAssignment struct {
	TopicPartitions *[]ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition // The assigned topic-partitions to the member. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition struct {
	TopicId         uuid.UUID // The topic ID. (versions: 0+)
	TopicName       *string   // The topic name. (versions: 0+)
	Partitions      *[]int32  // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *ConsumerGroupDescribeResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// Groups (versions: 0+)
	if res.Groups == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponse.Groups must not be nil in version %d", res.ApiVersion)
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
func (res *ConsumerGroupDescribeResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponse.Read: response or its body is nil")
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

func (res *ConsumerGroupDescribeResponse) groupsEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroup) error {
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
		return fmt.Errorf("ConsumerGroupDescribeResponseGroup.GroupId must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("ConsumerGroupDescribeResponseGroup.GroupState must not be nil in version %d", res.ApiVersion)
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

	// AssignorName (versions: 0+)
	if value.AssignorName == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroup.AssignorName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.AssignorName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.AssignorName); err != nil {
			return err
		}
	}

	// Members (versions: 0+)
	if value.Members == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroup.Members must not be nil in version %d", res.ApiVersion)
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

func (res *ConsumerGroupDescribeResponse) groupsDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroup, error) {
	consumergroupdescriberesponsegroup := ConsumerGroupDescribeResponseGroup{}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return consumergroupdescriberesponsegroup, err
	}
	consumergroupdescriberesponsegroup.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.ErrorMessage = errormessage
	}

	// GroupId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupid, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.GroupId = &groupid
	} else {
		groupid, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.GroupId = &groupid
	}

	// GroupState (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		groupstate, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.GroupState = &groupstate
	} else {
		groupstate, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.GroupState = &groupstate
	}

	// GroupEpoch (versions: 0+)
	groupepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return consumergroupdescriberesponsegroup, err
	}
	consumergroupdescriberesponsegroup.GroupEpoch = groupepoch

	// AssignmentEpoch (versions: 0+)
	assignmentepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return consumergroupdescriberesponsegroup, err
	}
	consumergroupdescriberesponsegroup.AssignmentEpoch = assignmentepoch

	// AssignorName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		assignorname, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.AssignorName = &assignorname
	} else {
		assignorname, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.AssignorName = &assignorname
	}

	// Members (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		members, err := protocol.ReadNullableCompactArray(r, res.membersDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.Members = members
	} else {
		members, err := protocol.ReadArray(r, res.membersDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.Members = &members
	}

	// AuthorizedOperations (versions: 0+)
	authorizedoperations, err := protocol.ReadInt32(r)
	if err != nil {
		return consumergroupdescriberesponsegroup, err
	}
	consumergroupdescriberesponsegroup.AuthorizedOperations = authorizedoperations

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroup, err
		}
		consumergroupdescriberesponsegroup.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroup, nil
}

func (res *ConsumerGroupDescribeResponse) membersEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroupMember) error {
	// MemberId (versions: 0+)
	if value.MemberId == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.MemberId must not be nil in version %d", res.ApiVersion)
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

	// MemberEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, value.MemberEpoch); err != nil {
		return err
	}

	// ClientId (versions: 0+)
	if value.ClientId == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.ClientId must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.ClientHost must not be nil in version %d", res.ApiVersion)
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

	// SubscribedTopicNames (versions: 0+)
	if value.SubscribedTopicNames == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.SubscribedTopicNames must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, value.SubscribedTopicNames); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *value.SubscribedTopicNames); err != nil {
			return err
		}
	}

	// SubscribedTopicRegex (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.SubscribedTopicRegex); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.SubscribedTopicRegex); err != nil {
			return err
		}
	}

	// Assignment (versions: 0+)
	if value.Assignment == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.Assignment must not be nil in version %d", res.ApiVersion)
	}
	if err := res.assignmentEncoder(w, *value.Assignment); err != nil {
		return err
	}

	// TargetAssignment (versions: 0+)
	if value.TargetAssignment == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMember.TargetAssignment must not be nil in version %d", res.ApiVersion)
	}
	if err := res.targetAssignmentEncoder(w, *value.TargetAssignment); err != nil {
		return err
	}

	// MemberType (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.MemberType); err != nil {
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

func (res *ConsumerGroupDescribeResponse) membersDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroupMember, error) {
	consumergroupdescriberesponsegroupmember := ConsumerGroupDescribeResponseGroupMember{}

	// MemberId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		memberid, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.MemberId = &memberid
	} else {
		memberid, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.MemberId = &memberid
	}

	// InstanceId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		instanceid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.InstanceId = instanceid
	} else {
		instanceid, err := protocol.ReadNullableString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.InstanceId = instanceid
	}

	// RackId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		rackid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.RackId = rackid
	} else {
		rackid, err := protocol.ReadNullableString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.RackId = rackid
	}

	// MemberEpoch (versions: 0+)
	memberepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return consumergroupdescriberesponsegroupmember, err
	}
	consumergroupdescriberesponsegroupmember.MemberEpoch = memberepoch

	// ClientId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clientid, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.ClientId = &clientid
	} else {
		clientid, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.ClientId = &clientid
	}

	// ClientHost (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		clienthost, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.ClientHost = &clienthost
	} else {
		clienthost, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.ClientHost = &clienthost
	}

	// SubscribedTopicNames (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subscribedtopicnames, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.SubscribedTopicNames = subscribedtopicnames
	} else {
		subscribedtopicnames, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.SubscribedTopicNames = &subscribedtopicnames
	}

	// SubscribedTopicRegex (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		subscribedtopicregex, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.SubscribedTopicRegex = subscribedtopicregex
	} else {
		subscribedtopicregex, err := protocol.ReadNullableString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.SubscribedTopicRegex = subscribedtopicregex
	}

	// Assignment (versions: 0+)
	assignment, err := res.assignmentDecoder(r)
	if err != nil {
		return consumergroupdescriberesponsegroupmember, err
	}
	consumergroupdescriberesponsegroupmember.Assignment = &assignment

	// TargetAssignment (versions: 0+)
	targetassignment, err := res.targetAssignmentDecoder(r)
	if err != nil {
		return consumergroupdescriberesponsegroupmember, err
	}
	consumergroupdescriberesponsegroupmember.TargetAssignment = &targetassignment

	// MemberType (versions: 1+)
	if res.ApiVersion >= 1 {
		membertype, err := protocol.ReadInt8(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.MemberType = membertype
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmember, err
		}
		consumergroupdescriberesponsegroupmember.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroupmember, nil
}

func (res *ConsumerGroupDescribeResponse) assignmentEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroupMemberAssignment) error {
	// TopicPartitions (versions: 0+)
	if value.TopicPartitions == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberAssignment.TopicPartitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicPartitionsEncoder, value.TopicPartitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicPartitionsEncoder, *value.TopicPartitions); err != nil {
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

func (res *ConsumerGroupDescribeResponse) assignmentDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroupMemberAssignment, error) {
	consumergroupdescriberesponsegroupmemberassignment := ConsumerGroupDescribeResponseGroupMemberAssignment{}

	// TopicPartitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicpartitions, err := protocol.ReadNullableCompactArray(r, res.topicPartitionsDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignment, err
		}
		consumergroupdescriberesponsegroupmemberassignment.TopicPartitions = topicpartitions
	} else {
		topicpartitions, err := protocol.ReadArray(r, res.topicPartitionsDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignment, err
		}
		consumergroupdescriberesponsegroupmemberassignment.TopicPartitions = &topicpartitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignment, err
		}
		consumergroupdescriberesponsegroupmemberassignment.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroupmemberassignment, nil
}

func (res *ConsumerGroupDescribeResponse) topicPartitionsEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition.TopicName must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *ConsumerGroupDescribeResponse) topicPartitionsDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition, error) {
	consumergroupdescriberesponsegroupmemberassignmenttopicpartition := ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
	}
	consumergroupdescriberesponsegroupmemberassignmenttopicpartition.TopicId = topicid

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmemberassignmenttopicpartition.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmemberassignmenttopicpartition.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmemberassignmenttopicpartition.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmemberassignmenttopicpartition.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmemberassignmenttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroupmemberassignmenttopicpartition, nil
}

func (res *ConsumerGroupDescribeResponse) targetAssignmentEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroupMemberTargetAssignment) error {
	// TopicPartitions (versions: 0+)
	if value.TopicPartitions == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberTargetAssignment.TopicPartitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionEncoder, value.TopicPartitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionEncoder, *value.TopicPartitions); err != nil {
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

func (res *ConsumerGroupDescribeResponse) targetAssignmentDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroupMemberTargetAssignment, error) {
	consumergroupdescriberesponsegroupmembertargetassignment := ConsumerGroupDescribeResponseGroupMemberTargetAssignment{}

	// TopicPartitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicpartitions, err := protocol.ReadNullableCompactArray(r, res.consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignment, err
		}
		consumergroupdescriberesponsegroupmembertargetassignment.TopicPartitions = topicpartitions
	} else {
		topicpartitions, err := protocol.ReadArray(r, res.consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionDecoder)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignment, err
		}
		consumergroupdescriberesponsegroupmembertargetassignment.TopicPartitions = &topicpartitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignment, err
		}
		consumergroupdescriberesponsegroupmembertargetassignment.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroupmembertargetassignment, nil
}

func (res *ConsumerGroupDescribeResponse) consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionEncoder(w io.Writer, value ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition.TopicName must not be nil in version %d", res.ApiVersion)
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
		return fmt.Errorf("ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *ConsumerGroupDescribeResponse) consumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartitionDecoder(r io.Reader) (ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition, error) {
	consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition := ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
	}
	consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.TopicId = topicid

	// TopicName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, err
		}
		consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return consumergroupdescriberesponsegroupmembertargetassignmenttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *ConsumerGroupDescribeResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- ConsumerGroupDescribeResponse:\n")
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
func (value *ConsumerGroupDescribeResponseGroup) PrettyPrint() string {
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

	if value.AssignorName != nil {
		fmt.Fprintf(w, "            AssignorName: %v\n", *value.AssignorName)
	} else {
		fmt.Fprintf(w, "            AssignorName: nil\n")
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
func (value *ConsumerGroupDescribeResponseGroupMember) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.MemberId != nil {
		fmt.Fprintf(w, "                MemberId: %v\n", *value.MemberId)
	} else {
		fmt.Fprintf(w, "                MemberId: nil\n")
	}

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

	fmt.Fprintf(w, "                MemberEpoch: %v\n", value.MemberEpoch)

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

	if value.SubscribedTopicNames != nil {
		fmt.Fprintf(w, "                SubscribedTopicNames: %v\n", *value.SubscribedTopicNames)
	} else {
		fmt.Fprintf(w, "                SubscribedTopicNames: nil\n")
	}

	if value.SubscribedTopicRegex != nil {
		fmt.Fprintf(w, "                SubscribedTopicRegex: %v\n", *value.SubscribedTopicRegex)
	} else {
		fmt.Fprintf(w, "                SubscribedTopicRegex: nil\n")
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

	fmt.Fprintf(w, "                MemberType: %v\n", value.MemberType)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ConsumerGroupDescribeResponseGroupMemberAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicPartitions != nil {
		fmt.Fprintf(w, "                    TopicPartitions:\n")
		for _, topicpartitions := range *value.TopicPartitions {
			fmt.Fprintf(w, "%s", topicpartitions.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    TopicPartitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ConsumerGroupDescribeResponseGroupMemberAssignmentTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                        TopicId: %v\n", value.TopicId)

	if value.TopicName != nil {
		fmt.Fprintf(w, "                        TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "                        TopicName: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ConsumerGroupDescribeResponseGroupMemberTargetAssignment) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.TopicPartitions != nil {
		fmt.Fprintf(w, "                    TopicPartitions:\n")
		for _, topicpartitions := range *value.TopicPartitions {
			fmt.Fprintf(w, "%s", topicpartitions.PrettyPrint())
			fmt.Fprintf(w, "                        ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                    TopicPartitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ConsumerGroupDescribeResponseGroupMemberTargetAssignmentTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                        TopicId: %v\n", value.TopicId)

	if value.TopicName != nil {
		fmt.Fprintf(w, "                        TopicName: %v\n", *value.TopicName)
	} else {
		fmt.Fprintf(w, "                        TopicName: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                        Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                        Partitions: nil\n")
	}

	return w.String()
}
