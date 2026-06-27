package endquorumepoch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EndQuorumEpochRequest struct {
	ApiVersion      int16
	ClusterId       *string                                // The cluster id. (versions: 0+, nullable: 0+)
	Topics          *[]EndQuorumEpochRequestTopic          // The topics. (versions: 0+)
	LeaderEndpoints *[]EndQuorumEpochRequestLeaderEndpoint // Endpoints for the leader. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type EndQuorumEpochRequestTopic struct {
	TopicName       *string                                // The topic name. (versions: 0+)
	Partitions      *[]EndQuorumEpochRequestTopicPartition // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type EndQuorumEpochRequestTopicPartition struct {
	PartitionIndex      int32                                                    // The partition index. (versions: 0+)
	LeaderId            int32                                                    // The current leader ID that is resigning. (versions: 0+)
	LeaderEpoch         int32                                                    // The current epoch. (versions: 0+)
	PreferredSuccessors *[]int32                                                 // A sorted list of preferred successors to start the election. (versions: 0)
	PreferredCandidates *[]EndQuorumEpochRequestTopicPartitionPreferredCandidate // A sorted list of preferred candidates to start the election. (versions: 1+)
	rawTaggedFields     *[]protocol.TaggedField
}

type EndQuorumEpochRequestTopicPartitionPreferredCandidate struct {
	CandidateId          int32     // The ID of the candidate replica. (versions: 1+)
	CandidateDirectoryId uuid.UUID // The directory ID of the candidate replica. (versions: 1+)
	rawTaggedFields      *[]protocol.TaggedField
}

type EndQuorumEpochRequestLeaderEndpoint struct {
	Name            *string // The name of the endpoint. (versions: 1+)
	Host            *string // The node's hostname. (versions: 1+)
	Port            uint16  // The node's port. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *EndQuorumEpochRequest) Write(w io.Writer) error {
	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.ClusterId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.ClusterId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("EndQuorumEpochRequest.Topics must not be nil in version %d", req.ApiVersion)
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

	// LeaderEndpoints (versions: 1+)
	if req.ApiVersion >= 1 {
		if req.LeaderEndpoints == nil {
			return fmt.Errorf("EndQuorumEpochRequest.LeaderEndpoints must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.leaderEndpointsEncoder, req.LeaderEndpoints); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.leaderEndpointsEncoder, *req.LeaderEndpoints); err != nil {
				return err
			}
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
func (req *EndQuorumEpochRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("EndQuorumEpochRequest.Read: request or its body is nil")
	}

	*req = EndQuorumEpochRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		clusterid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	} else {
		clusterid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
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

	// LeaderEndpoints (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			leaderendpoints, err := protocol.ReadCompactArray(r, req.leaderEndpointsDecoder)
			if err != nil {
				return err
			}
			req.LeaderEndpoints = &leaderendpoints
		} else {
			leaderendpoints, err := protocol.ReadArray(r, req.leaderEndpointsDecoder)
			if err != nil {
				return err
			}
			req.LeaderEndpoints = &leaderendpoints
		}
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

func (req *EndQuorumEpochRequest) topicsEncoder(w io.Writer, value EndQuorumEpochRequestTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("EndQuorumEpochRequestTopic.TopicName must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("EndQuorumEpochRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *EndQuorumEpochRequest) topicsDecoder(r io.Reader) (EndQuorumEpochRequestTopic, error) {
	endquorumepochrequesttopic := EndQuorumEpochRequestTopic{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return endquorumepochrequesttopic, err
		}
		endquorumepochrequesttopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return endquorumepochrequesttopic, err
		}
		endquorumepochrequesttopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return endquorumepochrequesttopic, err
		}
		endquorumepochrequesttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return endquorumepochrequesttopic, err
		}
		endquorumepochrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochrequesttopic, err
		}
		endquorumepochrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochrequesttopic, nil
}

func (req *EndQuorumEpochRequest) partitionsEncoder(w io.Writer, value EndQuorumEpochRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
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

	// PreferredSuccessors (versions: 0)
	if req.ApiVersion == 0 {
		if value.PreferredSuccessors == nil {
			return fmt.Errorf("EndQuorumEpochRequestTopicPartition.PreferredSuccessors must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.PreferredSuccessors); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteInt32, *value.PreferredSuccessors); err != nil {
				return err
			}
		}
	}

	// PreferredCandidates (versions: 1+)
	if req.ApiVersion >= 1 {
		if value.PreferredCandidates == nil {
			return fmt.Errorf("EndQuorumEpochRequestTopicPartition.PreferredCandidates must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, req.preferredCandidatesEncoder, value.PreferredCandidates); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, req.preferredCandidatesEncoder, *value.PreferredCandidates); err != nil {
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

func (req *EndQuorumEpochRequest) partitionsDecoder(r io.Reader) (EndQuorumEpochRequestTopicPartition, error) {
	endquorumepochrequesttopicpartition := EndQuorumEpochRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochrequesttopicpartition, err
	}
	endquorumepochrequesttopicpartition.PartitionIndex = partitionindex

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochrequesttopicpartition, err
	}
	endquorumepochrequesttopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return endquorumepochrequesttopicpartition, err
	}
	endquorumepochrequesttopicpartition.LeaderEpoch = leaderepoch

	// PreferredSuccessors (versions: 0)
	if req.ApiVersion == 0 {
		if isRequestFlexible(req.ApiVersion) {
			preferredsuccessors, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
			if err != nil {
				return endquorumepochrequesttopicpartition, err
			}
			endquorumepochrequesttopicpartition.PreferredSuccessors = &preferredsuccessors
		} else {
			preferredsuccessors, err := protocol.ReadArray(r, protocol.ReadInt32)
			if err != nil {
				return endquorumepochrequesttopicpartition, err
			}
			endquorumepochrequesttopicpartition.PreferredSuccessors = &preferredsuccessors
		}
	}

	// PreferredCandidates (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			preferredcandidates, err := protocol.ReadCompactArray(r, req.preferredCandidatesDecoder)
			if err != nil {
				return endquorumepochrequesttopicpartition, err
			}
			endquorumepochrequesttopicpartition.PreferredCandidates = &preferredcandidates
		} else {
			preferredcandidates, err := protocol.ReadArray(r, req.preferredCandidatesDecoder)
			if err != nil {
				return endquorumepochrequesttopicpartition, err
			}
			endquorumepochrequesttopicpartition.PreferredCandidates = &preferredcandidates
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochrequesttopicpartition, err
		}
		endquorumepochrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochrequesttopicpartition, nil
}

func (req *EndQuorumEpochRequest) preferredCandidatesEncoder(w io.Writer, value EndQuorumEpochRequestTopicPartitionPreferredCandidate) error {
	// CandidateId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, value.CandidateId); err != nil {
			return err
		}
	}

	// CandidateDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteUUID(w, value.CandidateDirectoryId); err != nil {
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

func (req *EndQuorumEpochRequest) preferredCandidatesDecoder(r io.Reader) (EndQuorumEpochRequestTopicPartitionPreferredCandidate, error) {
	endquorumepochrequesttopicpartitionpreferredcandidate := EndQuorumEpochRequestTopicPartitionPreferredCandidate{}

	// CandidateId (versions: 1+)
	if req.ApiVersion >= 1 {
		candidateid, err := protocol.ReadInt32(r)
		if err != nil {
			return endquorumepochrequesttopicpartitionpreferredcandidate, err
		}
		endquorumepochrequesttopicpartitionpreferredcandidate.CandidateId = candidateid
	}

	// CandidateDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		candidatedirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return endquorumepochrequesttopicpartitionpreferredcandidate, err
		}
		endquorumepochrequesttopicpartitionpreferredcandidate.CandidateDirectoryId = candidatedirectoryid
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochrequesttopicpartitionpreferredcandidate, err
		}
		endquorumepochrequesttopicpartitionpreferredcandidate.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochrequesttopicpartitionpreferredcandidate, nil
}

func (req *EndQuorumEpochRequest) leaderEndpointsEncoder(w io.Writer, value EndQuorumEpochRequestLeaderEndpoint) error {
	// Name (versions: 1+)
	if req.ApiVersion >= 1 {
		if value.Name == nil {
			return fmt.Errorf("EndQuorumEpochRequestLeaderEndpoint.Name must not be nil in version %d", req.ApiVersion)
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

	// Host (versions: 1+)
	if req.ApiVersion >= 1 {
		if value.Host == nil {
			return fmt.Errorf("EndQuorumEpochRequestLeaderEndpoint.Host must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
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
	if req.ApiVersion >= 1 {
		if err := protocol.WriteUint16(w, value.Port); err != nil {
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

func (req *EndQuorumEpochRequest) leaderEndpointsDecoder(r io.Reader) (EndQuorumEpochRequestLeaderEndpoint, error) {
	endquorumepochrequestleaderendpoint := EndQuorumEpochRequestLeaderEndpoint{}

	// Name (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return endquorumepochrequestleaderendpoint, err
			}
			endquorumepochrequestleaderendpoint.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return endquorumepochrequestleaderendpoint, err
			}
			endquorumepochrequestleaderendpoint.Name = &name
		}
	}

	// Host (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return endquorumepochrequestleaderendpoint, err
			}
			endquorumepochrequestleaderendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return endquorumepochrequestleaderendpoint, err
			}
			endquorumepochrequestleaderendpoint.Host = &host
		}
	}

	// Port (versions: 1+)
	if req.ApiVersion >= 1 {
		port, err := protocol.ReadUInt16(r)
		if err != nil {
			return endquorumepochrequestleaderendpoint, err
		}
		endquorumepochrequestleaderendpoint.Port = port
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return endquorumepochrequestleaderendpoint, err
		}
		endquorumepochrequestleaderendpoint.rawTaggedFields = &rawTaggedFields
	}

	return endquorumepochrequestleaderendpoint, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *EndQuorumEpochRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> EndQuorumEpochRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
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

	if req.LeaderEndpoints != nil {
		fmt.Fprintf(w, "        LeaderEndpoints:\n")
		for _, leaderendpoints := range *req.LeaderEndpoints {
			fmt.Fprintf(w, "%s", leaderendpoints.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        LeaderEndpoints: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *EndQuorumEpochRequestTopic) PrettyPrint() string {
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
func (value *EndQuorumEpochRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	if value.PreferredSuccessors != nil {
		fmt.Fprintf(w, "                PreferredSuccessors: %v\n", *value.PreferredSuccessors)
	} else {
		fmt.Fprintf(w, "                PreferredSuccessors: nil\n")
	}

	if value.PreferredCandidates != nil {
		fmt.Fprintf(w, "                PreferredCandidates:\n")
		for _, preferredcandidates := range *value.PreferredCandidates {
			fmt.Fprintf(w, "%s", preferredcandidates.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                PreferredCandidates: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *EndQuorumEpochRequestTopicPartitionPreferredCandidate) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    CandidateId: %v\n", value.CandidateId)
	fmt.Fprintf(w, "                    CandidateDirectoryId: %v\n", value.CandidateDirectoryId)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *EndQuorumEpochRequestLeaderEndpoint) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}

	fmt.Fprintf(w, "            Port: %v\n", value.Port)

	return w.String()
}
