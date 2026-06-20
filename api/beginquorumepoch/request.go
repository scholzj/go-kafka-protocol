package beginquorumepoch

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type BeginQuorumEpochRequest struct {
	ApiVersion      int16
	ClusterId       *string                                  // The cluster id. (versions: 0+, nullable: 0+)
	VoterId         int32                                    // The replica id of the voter receiving the request. (versions: 1+)
	Topics          *[]BeginQuorumEpochRequestTopic          // The topics. (versions: 0+)
	LeaderEndpoints *[]BeginQuorumEpochRequestLeaderEndpoint // Endpoints for the leader. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type BeginQuorumEpochRequestTopic struct {
	TopicName       *string                                  // The topic name. (versions: 0+)
	Partitions      *[]BeginQuorumEpochRequestTopicPartition // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type BeginQuorumEpochRequestTopicPartition struct {
	PartitionIndex   int32     // The partition index. (versions: 0+)
	VoterDirectoryId uuid.UUID // The directory id of the receiving replica. (versions: 1+)
	LeaderId         int32     // The ID of the newly elected leader. (versions: 0+)
	LeaderEpoch      int32     // The epoch of the newly elected leader. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type BeginQuorumEpochRequestLeaderEndpoint struct {
	Name            *string // The name of the endpoint. (versions: 1+)
	Host            *string // The node's hostname. (versions: 1+)
	Port            uint16  // The node's port. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 1
}

func (req *BeginQuorumEpochRequest) Write(w io.Writer) error {
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

	// VoterId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, req.VoterId); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if req.Topics == nil {
		return fmt.Errorf("BeginQuorumEpochRequest.Topics must not be nil in version %d", req.ApiVersion)
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
			return fmt.Errorf("BeginQuorumEpochRequest.LeaderEndpoints must not be nil in version %d", req.ApiVersion)
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
func (req *BeginQuorumEpochRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("BeginQuorumEpochRequest.Read: request or its body is nil")
	}

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

	// VoterId (versions: 1+)
	if req.ApiVersion >= 1 {
		voterid, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		req.VoterId = voterid
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
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

	// LeaderEndpoints (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			leaderendpoints, err := protocol.ReadNullableCompactArray(r, req.leaderEndpointsDecoder)
			if err != nil {
				return err
			}
			req.LeaderEndpoints = leaderendpoints
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

func (req *BeginQuorumEpochRequest) topicsEncoder(w io.Writer, value BeginQuorumEpochRequestTopic) error {
	// TopicName (versions: 0+)
	if value.TopicName == nil {
		return fmt.Errorf("BeginQuorumEpochRequestTopic.TopicName must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("BeginQuorumEpochRequestTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *BeginQuorumEpochRequest) topicsDecoder(r io.Reader) (BeginQuorumEpochRequestTopic, error) {
	beginquorumepochrequesttopic := BeginQuorumEpochRequestTopic{}

	// TopicName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topicname, err := protocol.ReadCompactString(r)
		if err != nil {
			return beginquorumepochrequesttopic, err
		}
		beginquorumepochrequesttopic.TopicName = &topicname
	} else {
		topicname, err := protocol.ReadString(r)
		if err != nil {
			return beginquorumepochrequesttopic, err
		}
		beginquorumepochrequesttopic.TopicName = &topicname
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return beginquorumepochrequesttopic, err
		}
		beginquorumepochrequesttopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return beginquorumepochrequesttopic, err
		}
		beginquorumepochrequesttopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return beginquorumepochrequesttopic, err
		}
		beginquorumepochrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return beginquorumepochrequesttopic, nil
}

func (req *BeginQuorumEpochRequest) partitionsEncoder(w io.Writer, value BeginQuorumEpochRequestTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// VoterDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteUUID(w, value.VoterDirectoryId); err != nil {
			return err
		}
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

func (req *BeginQuorumEpochRequest) partitionsDecoder(r io.Reader) (BeginQuorumEpochRequestTopicPartition, error) {
	beginquorumepochrequesttopicpartition := BeginQuorumEpochRequestTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return beginquorumepochrequesttopicpartition, err
	}
	beginquorumepochrequesttopicpartition.PartitionIndex = partitionindex

	// VoterDirectoryId (versions: 1+)
	if req.ApiVersion >= 1 {
		voterdirectoryid, err := protocol.ReadUUID(r)
		if err != nil {
			return beginquorumepochrequesttopicpartition, err
		}
		beginquorumepochrequesttopicpartition.VoterDirectoryId = voterdirectoryid
	}

	// LeaderId (versions: 0+)
	leaderid, err := protocol.ReadInt32(r)
	if err != nil {
		return beginquorumepochrequesttopicpartition, err
	}
	beginquorumepochrequesttopicpartition.LeaderId = leaderid

	// LeaderEpoch (versions: 0+)
	leaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return beginquorumepochrequesttopicpartition, err
	}
	beginquorumepochrequesttopicpartition.LeaderEpoch = leaderepoch

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return beginquorumepochrequesttopicpartition, err
		}
		beginquorumepochrequesttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return beginquorumepochrequesttopicpartition, nil
}

func (req *BeginQuorumEpochRequest) leaderEndpointsEncoder(w io.Writer, value BeginQuorumEpochRequestLeaderEndpoint) error {
	// Name (versions: 1+)
	if req.ApiVersion >= 1 {
		if value.Name == nil {
			return fmt.Errorf("BeginQuorumEpochRequestLeaderEndpoint.Name must not be nil in version %d", req.ApiVersion)
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
			return fmt.Errorf("BeginQuorumEpochRequestLeaderEndpoint.Host must not be nil in version %d", req.ApiVersion)
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

func (req *BeginQuorumEpochRequest) leaderEndpointsDecoder(r io.Reader) (BeginQuorumEpochRequestLeaderEndpoint, error) {
	beginquorumepochrequestleaderendpoint := BeginQuorumEpochRequestLeaderEndpoint{}

	// Name (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			name, err := protocol.ReadCompactString(r)
			if err != nil {
				return beginquorumepochrequestleaderendpoint, err
			}
			beginquorumepochrequestleaderendpoint.Name = &name
		} else {
			name, err := protocol.ReadString(r)
			if err != nil {
				return beginquorumepochrequestleaderendpoint, err
			}
			beginquorumepochrequestleaderendpoint.Name = &name
		}
	}

	// Host (versions: 1+)
	if req.ApiVersion >= 1 {
		if isRequestFlexible(req.ApiVersion) {
			host, err := protocol.ReadCompactString(r)
			if err != nil {
				return beginquorumepochrequestleaderendpoint, err
			}
			beginquorumepochrequestleaderendpoint.Host = &host
		} else {
			host, err := protocol.ReadString(r)
			if err != nil {
				return beginquorumepochrequestleaderendpoint, err
			}
			beginquorumepochrequestleaderendpoint.Host = &host
		}
	}

	// Port (versions: 1+)
	if req.ApiVersion >= 1 {
		port, err := protocol.ReadUInt16(r)
		if err != nil {
			return beginquorumepochrequestleaderendpoint, err
		}
		beginquorumepochrequestleaderendpoint.Port = port
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return beginquorumepochrequestleaderendpoint, err
		}
		beginquorumepochrequestleaderendpoint.rawTaggedFields = &rawTaggedFields
	}

	return beginquorumepochrequestleaderendpoint, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *BeginQuorumEpochRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> BeginQuorumEpochRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        VoterId: %v\n", req.VoterId)

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
func (value *BeginQuorumEpochRequestTopic) PrettyPrint() string {
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
func (value *BeginQuorumEpochRequestTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                VoterDirectoryId: %v\n", value.VoterDirectoryId)
	fmt.Fprintf(w, "                LeaderId: %v\n", value.LeaderId)
	fmt.Fprintf(w, "                LeaderEpoch: %v\n", value.LeaderEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *BeginQuorumEpochRequestLeaderEndpoint) PrettyPrint() string {
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
