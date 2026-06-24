package messages

import (
	"github.com/scholzj/go-kafka-protocol/protocol"

	"github.com/scholzj/go-kafka-protocol/api/addoffsetstotxn"
	"github.com/scholzj/go-kafka-protocol/api/addpartitionstotxn"
	"github.com/scholzj/go-kafka-protocol/api/addraftvoter"
	"github.com/scholzj/go-kafka-protocol/api/allocateproducerids"
	"github.com/scholzj/go-kafka-protocol/api/alterclientquotas"
	"github.com/scholzj/go-kafka-protocol/api/alterconfigs"
	"github.com/scholzj/go-kafka-protocol/api/alterpartition"
	"github.com/scholzj/go-kafka-protocol/api/alterpartitionreassignments"
	"github.com/scholzj/go-kafka-protocol/api/alterreplicalogdirs"
	"github.com/scholzj/go-kafka-protocol/api/altersharegroupoffsets"
	"github.com/scholzj/go-kafka-protocol/api/alteruserscramcredentials"
	"github.com/scholzj/go-kafka-protocol/api/apiversions"
	"github.com/scholzj/go-kafka-protocol/api/assignreplicastodirs"
	"github.com/scholzj/go-kafka-protocol/api/beginquorumepoch"
	"github.com/scholzj/go-kafka-protocol/api/brokerheartbeat"
	"github.com/scholzj/go-kafka-protocol/api/brokerregistration"
	"github.com/scholzj/go-kafka-protocol/api/consumergroupdescribe"
	"github.com/scholzj/go-kafka-protocol/api/consumergroupheartbeat"
	"github.com/scholzj/go-kafka-protocol/api/controllerregistration"
	"github.com/scholzj/go-kafka-protocol/api/createacls"
	"github.com/scholzj/go-kafka-protocol/api/createdelegationtoken"
	"github.com/scholzj/go-kafka-protocol/api/createpartitions"
	"github.com/scholzj/go-kafka-protocol/api/createtopics"
	"github.com/scholzj/go-kafka-protocol/api/deleteacls"
	"github.com/scholzj/go-kafka-protocol/api/deletegroups"
	"github.com/scholzj/go-kafka-protocol/api/deleterecords"
	"github.com/scholzj/go-kafka-protocol/api/deletesharegroupoffsets"
	"github.com/scholzj/go-kafka-protocol/api/deletesharegroupstate"
	"github.com/scholzj/go-kafka-protocol/api/deletetopics"
	"github.com/scholzj/go-kafka-protocol/api/describeacls"
	"github.com/scholzj/go-kafka-protocol/api/describeclientquotas"
	"github.com/scholzj/go-kafka-protocol/api/describecluster"
	"github.com/scholzj/go-kafka-protocol/api/describeconfigs"
	"github.com/scholzj/go-kafka-protocol/api/describedelegationtoken"
	"github.com/scholzj/go-kafka-protocol/api/describegroups"
	"github.com/scholzj/go-kafka-protocol/api/describelogdirs"
	"github.com/scholzj/go-kafka-protocol/api/describeproducers"
	"github.com/scholzj/go-kafka-protocol/api/describequorum"
	"github.com/scholzj/go-kafka-protocol/api/describesharegroupoffsets"
	"github.com/scholzj/go-kafka-protocol/api/describetopicpartitions"
	"github.com/scholzj/go-kafka-protocol/api/describetransactions"
	"github.com/scholzj/go-kafka-protocol/api/describeuserscramcredentials"
	"github.com/scholzj/go-kafka-protocol/api/electleaders"
	"github.com/scholzj/go-kafka-protocol/api/endquorumepoch"
	"github.com/scholzj/go-kafka-protocol/api/endtxn"
	"github.com/scholzj/go-kafka-protocol/api/envelope"
	"github.com/scholzj/go-kafka-protocol/api/expiredelegationtoken"
	"github.com/scholzj/go-kafka-protocol/api/fetch"
	"github.com/scholzj/go-kafka-protocol/api/fetchsnapshot"
	"github.com/scholzj/go-kafka-protocol/api/findcoordinator"
	"github.com/scholzj/go-kafka-protocol/api/gettelemetrysubscriptions"
	"github.com/scholzj/go-kafka-protocol/api/heartbeat"
	"github.com/scholzj/go-kafka-protocol/api/incrementalalterconfigs"
	"github.com/scholzj/go-kafka-protocol/api/initializesharegroupstate"
	"github.com/scholzj/go-kafka-protocol/api/initproducerid"
	"github.com/scholzj/go-kafka-protocol/api/joingroup"
	"github.com/scholzj/go-kafka-protocol/api/leavegroup"
	"github.com/scholzj/go-kafka-protocol/api/listconfigresources"
	"github.com/scholzj/go-kafka-protocol/api/listgroups"
	"github.com/scholzj/go-kafka-protocol/api/listoffsets"
	"github.com/scholzj/go-kafka-protocol/api/listpartitionreassignments"
	"github.com/scholzj/go-kafka-protocol/api/listtransactions"
	"github.com/scholzj/go-kafka-protocol/api/metadata"
	"github.com/scholzj/go-kafka-protocol/api/offsetcommit"
	"github.com/scholzj/go-kafka-protocol/api/offsetdelete"
	"github.com/scholzj/go-kafka-protocol/api/offsetfetch"
	"github.com/scholzj/go-kafka-protocol/api/offsetforleaderepoch"
	"github.com/scholzj/go-kafka-protocol/api/produce"
	"github.com/scholzj/go-kafka-protocol/api/pushtelemetry"
	"github.com/scholzj/go-kafka-protocol/api/readsharegroupstate"
	"github.com/scholzj/go-kafka-protocol/api/readsharegroupstatesummary"
	"github.com/scholzj/go-kafka-protocol/api/removeraftvoter"
	"github.com/scholzj/go-kafka-protocol/api/renewdelegationtoken"
	"github.com/scholzj/go-kafka-protocol/api/saslauthenticate"
	"github.com/scholzj/go-kafka-protocol/api/saslhandshake"
	"github.com/scholzj/go-kafka-protocol/api/shareacknowledge"
	"github.com/scholzj/go-kafka-protocol/api/sharefetch"
	"github.com/scholzj/go-kafka-protocol/api/sharegroupdescribe"
	"github.com/scholzj/go-kafka-protocol/api/sharegroupheartbeat"
	"github.com/scholzj/go-kafka-protocol/api/streamsgroupdescribe"
	"github.com/scholzj/go-kafka-protocol/api/streamsgroupheartbeat"
	"github.com/scholzj/go-kafka-protocol/api/syncgroup"
	"github.com/scholzj/go-kafka-protocol/api/txnoffsetcommit"
	"github.com/scholzj/go-kafka-protocol/api/unregisterbroker"
	"github.com/scholzj/go-kafka-protocol/api/updatefeatures"
	"github.com/scholzj/go-kafka-protocol/api/updateraftvoter"
	"github.com/scholzj/go-kafka-protocol/api/vote"
	"github.com/scholzj/go-kafka-protocol/api/writesharegroupstate"
	"github.com/scholzj/go-kafka-protocol/api/writetxnmarkers"
)

// Named constants for every supported Kafka API key.
const (
	Produce                      int16 = 0
	Fetch                        int16 = 1
	ListOffsets                  int16 = 2
	Metadata                     int16 = 3
	OffsetCommit                 int16 = 8
	OffsetFetch                  int16 = 9
	FindCoordinator              int16 = 10
	JoinGroup                    int16 = 11
	Heartbeat                    int16 = 12
	LeaveGroup                   int16 = 13
	SyncGroup                    int16 = 14
	DescribeGroups               int16 = 15
	ListGroups                   int16 = 16
	SaslHandshake                int16 = 17
	ApiVersions                  int16 = 18
	CreateTopics                 int16 = 19
	DeleteTopics                 int16 = 20
	DeleteRecords                int16 = 21
	InitProducerId               int16 = 22
	OffsetForLeaderEpoch         int16 = 23
	AddPartitionsToTxn           int16 = 24
	AddOffsetsToTxn              int16 = 25
	EndTxn                       int16 = 26
	WriteTxnMarkers              int16 = 27
	TxnOffsetCommit              int16 = 28
	DescribeAcls                 int16 = 29
	CreateAcls                   int16 = 30
	DeleteAcls                   int16 = 31
	DescribeConfigs              int16 = 32
	AlterConfigs                 int16 = 33
	AlterReplicaLogDirs          int16 = 34
	DescribeLogDirs              int16 = 35
	SaslAuthenticate             int16 = 36
	CreatePartitions             int16 = 37
	CreateDelegationToken        int16 = 38
	RenewDelegationToken         int16 = 39
	ExpireDelegationToken        int16 = 40
	DescribeDelegationToken      int16 = 41
	DeleteGroups                 int16 = 42
	ElectLeaders                 int16 = 43
	IncrementalAlterConfigs      int16 = 44
	AlterPartitionReassignments  int16 = 45
	ListPartitionReassignments   int16 = 46
	OffsetDelete                 int16 = 47
	DescribeClientQuotas         int16 = 48
	AlterClientQuotas            int16 = 49
	DescribeUserScramCredentials int16 = 50
	AlterUserScramCredentials    int16 = 51
	Vote                         int16 = 52
	BeginQuorumEpoch             int16 = 53
	EndQuorumEpoch               int16 = 54
	DescribeQuorum               int16 = 55
	AlterPartition               int16 = 56
	UpdateFeatures               int16 = 57
	Envelope                     int16 = 58
	FetchSnapshot                int16 = 59
	DescribeCluster              int16 = 60
	DescribeProducers            int16 = 61
	BrokerRegistration           int16 = 62
	BrokerHeartbeat              int16 = 63
	UnregisterBroker             int16 = 64
	DescribeTransactions         int16 = 65
	ListTransactions             int16 = 66
	AllocateProducerIds          int16 = 67
	ConsumerGroupHeartbeat       int16 = 68
	ConsumerGroupDescribe        int16 = 69
	ControllerRegistration       int16 = 70
	GetTelemetrySubscriptions    int16 = 71
	PushTelemetry                int16 = 72
	AssignReplicasToDirs         int16 = 73
	ListConfigResources          int16 = 74
	DescribeTopicPartitions      int16 = 75
	ShareGroupHeartbeat          int16 = 76
	ShareGroupDescribe           int16 = 77
	ShareFetch                   int16 = 78
	ShareAcknowledge             int16 = 79
	AddRaftVoter                 int16 = 80
	RemoveRaftVoter              int16 = 81
	UpdateRaftVoter              int16 = 82
	InitializeShareGroupState    int16 = 83
	ReadShareGroupState          int16 = 84
	WriteShareGroupState         int16 = 85
	DeleteShareGroupState        int16 = 86
	ReadShareGroupStateSummary   int16 = 87
	StreamsGroupHeartbeat        int16 = 88
	StreamsGroupDescribe         int16 = 89
	DescribeShareGroupOffsets    int16 = 90
	AlterShareGroupOffsets       int16 = 91
	DeleteShareGroupOffsets      int16 = 92
)

// NewRequestBody returns an empty request body struct for the given API key, ready to be
// populated via its Read method. The boolean is false for unknown API keys.
func NewRequestBody(apiKey int16) (protocol.RequestBody, bool) {
	switch apiKey {
	case 0:
		return &produce.ProduceRequest{}, true
	case 1:
		return &fetch.FetchRequest{}, true
	case 2:
		return &listoffsets.ListOffsetsRequest{}, true
	case 3:
		return &metadata.MetadataRequest{}, true
	case 8:
		return &offsetcommit.OffsetCommitRequest{}, true
	case 9:
		return &offsetfetch.OffsetFetchRequest{}, true
	case 10:
		return &findcoordinator.FindCoordinatorRequest{}, true
	case 11:
		return &joingroup.JoinGroupRequest{}, true
	case 12:
		return &heartbeat.HeartbeatRequest{}, true
	case 13:
		return &leavegroup.LeaveGroupRequest{}, true
	case 14:
		return &syncgroup.SyncGroupRequest{}, true
	case 15:
		return &describegroups.DescribeGroupsRequest{}, true
	case 16:
		return &listgroups.ListGroupsRequest{}, true
	case 17:
		return &saslhandshake.SaslHandshakeRequest{}, true
	case 18:
		return &apiversions.ApiVersionsRequest{}, true
	case 19:
		return &createtopics.CreateTopicsRequest{}, true
	case 20:
		return &deletetopics.DeleteTopicsRequest{}, true
	case 21:
		return &deleterecords.DeleteRecordsRequest{}, true
	case 22:
		return &initproducerid.InitProducerIdRequest{}, true
	case 23:
		return &offsetforleaderepoch.OffsetForLeaderEpochRequest{}, true
	case 24:
		return &addpartitionstotxn.AddPartitionsToTxnRequest{}, true
	case 25:
		return &addoffsetstotxn.AddOffsetsToTxnRequest{}, true
	case 26:
		return &endtxn.EndTxnRequest{}, true
	case 27:
		return &writetxnmarkers.WriteTxnMarkersRequest{}, true
	case 28:
		return &txnoffsetcommit.TxnOffsetCommitRequest{}, true
	case 29:
		return &describeacls.DescribeAclsRequest{}, true
	case 30:
		return &createacls.CreateAclsRequest{}, true
	case 31:
		return &deleteacls.DeleteAclsRequest{}, true
	case 32:
		return &describeconfigs.DescribeConfigsRequest{}, true
	case 33:
		return &alterconfigs.AlterConfigsRequest{}, true
	case 34:
		return &alterreplicalogdirs.AlterReplicaLogDirsRequest{}, true
	case 35:
		return &describelogdirs.DescribeLogDirsRequest{}, true
	case 36:
		return &saslauthenticate.SaslAuthenticateRequest{}, true
	case 37:
		return &createpartitions.CreatePartitionsRequest{}, true
	case 38:
		return &createdelegationtoken.CreateDelegationTokenRequest{}, true
	case 39:
		return &renewdelegationtoken.RenewDelegationTokenRequest{}, true
	case 40:
		return &expiredelegationtoken.ExpireDelegationTokenRequest{}, true
	case 41:
		return &describedelegationtoken.DescribeDelegationTokenRequest{}, true
	case 42:
		return &deletegroups.DeleteGroupsRequest{}, true
	case 43:
		return &electleaders.ElectLeadersRequest{}, true
	case 44:
		return &incrementalalterconfigs.IncrementalAlterConfigsRequest{}, true
	case 45:
		return &alterpartitionreassignments.AlterPartitionReassignmentsRequest{}, true
	case 46:
		return &listpartitionreassignments.ListPartitionReassignmentsRequest{}, true
	case 47:
		return &offsetdelete.OffsetDeleteRequest{}, true
	case 48:
		return &describeclientquotas.DescribeClientQuotasRequest{}, true
	case 49:
		return &alterclientquotas.AlterClientQuotasRequest{}, true
	case 50:
		return &describeuserscramcredentials.DescribeUserScramCredentialsRequest{}, true
	case 51:
		return &alteruserscramcredentials.AlterUserScramCredentialsRequest{}, true
	case 52:
		return &vote.VoteRequest{}, true
	case 53:
		return &beginquorumepoch.BeginQuorumEpochRequest{}, true
	case 54:
		return &endquorumepoch.EndQuorumEpochRequest{}, true
	case 55:
		return &describequorum.DescribeQuorumRequest{}, true
	case 56:
		return &alterpartition.AlterPartitionRequest{}, true
	case 57:
		return &updatefeatures.UpdateFeaturesRequest{}, true
	case 58:
		return &envelope.EnvelopeRequest{}, true
	case 59:
		return &fetchsnapshot.FetchSnapshotRequest{}, true
	case 60:
		return &describecluster.DescribeClusterRequest{}, true
	case 61:
		return &describeproducers.DescribeProducersRequest{}, true
	case 62:
		return &brokerregistration.BrokerRegistrationRequest{}, true
	case 63:
		return &brokerheartbeat.BrokerHeartbeatRequest{}, true
	case 64:
		return &unregisterbroker.UnregisterBrokerRequest{}, true
	case 65:
		return &describetransactions.DescribeTransactionsRequest{}, true
	case 66:
		return &listtransactions.ListTransactionsRequest{}, true
	case 67:
		return &allocateproducerids.AllocateProducerIdsRequest{}, true
	case 68:
		return &consumergroupheartbeat.ConsumerGroupHeartbeatRequest{}, true
	case 69:
		return &consumergroupdescribe.ConsumerGroupDescribeRequest{}, true
	case 70:
		return &controllerregistration.ControllerRegistrationRequest{}, true
	case 71:
		return &gettelemetrysubscriptions.GetTelemetrySubscriptionsRequest{}, true
	case 72:
		return &pushtelemetry.PushTelemetryRequest{}, true
	case 73:
		return &assignreplicastodirs.AssignReplicasToDirsRequest{}, true
	case 74:
		return &listconfigresources.ListConfigResourcesRequest{}, true
	case 75:
		return &describetopicpartitions.DescribeTopicPartitionsRequest{}, true
	case 76:
		return &sharegroupheartbeat.ShareGroupHeartbeatRequest{}, true
	case 77:
		return &sharegroupdescribe.ShareGroupDescribeRequest{}, true
	case 78:
		return &sharefetch.ShareFetchRequest{}, true
	case 79:
		return &shareacknowledge.ShareAcknowledgeRequest{}, true
	case 80:
		return &addraftvoter.AddRaftVoterRequest{}, true
	case 81:
		return &removeraftvoter.RemoveRaftVoterRequest{}, true
	case 82:
		return &updateraftvoter.UpdateRaftVoterRequest{}, true
	case 83:
		return &initializesharegroupstate.InitializeShareGroupStateRequest{}, true
	case 84:
		return &readsharegroupstate.ReadShareGroupStateRequest{}, true
	case 85:
		return &writesharegroupstate.WriteShareGroupStateRequest{}, true
	case 86:
		return &deletesharegroupstate.DeleteShareGroupStateRequest{}, true
	case 87:
		return &readsharegroupstatesummary.ReadShareGroupStateSummaryRequest{}, true
	case 88:
		return &streamsgroupheartbeat.StreamsGroupHeartbeatRequest{}, true
	case 89:
		return &streamsgroupdescribe.StreamsGroupDescribeRequest{}, true
	case 90:
		return &describesharegroupoffsets.DescribeShareGroupOffsetsRequest{}, true
	case 91:
		return &altersharegroupoffsets.AlterShareGroupOffsetsRequest{}, true
	case 92:
		return &deletesharegroupoffsets.DeleteShareGroupOffsetsRequest{}, true
	default:
		return nil, false
	}
}

// NewResponseBody returns an empty response body struct for the given API key, ready to be
// populated via its Read method. The boolean is false for unknown API keys.
func NewResponseBody(apiKey int16) (protocol.ResponseBody, bool) {
	switch apiKey {
	case 0:
		return &produce.ProduceResponse{}, true
	case 1:
		return &fetch.FetchResponse{}, true
	case 2:
		return &listoffsets.ListOffsetsResponse{}, true
	case 3:
		return &metadata.MetadataResponse{}, true
	case 8:
		return &offsetcommit.OffsetCommitResponse{}, true
	case 9:
		return &offsetfetch.OffsetFetchResponse{}, true
	case 10:
		return &findcoordinator.FindCoordinatorResponse{}, true
	case 11:
		return &joingroup.JoinGroupResponse{}, true
	case 12:
		return &heartbeat.HeartbeatResponse{}, true
	case 13:
		return &leavegroup.LeaveGroupResponse{}, true
	case 14:
		return &syncgroup.SyncGroupResponse{}, true
	case 15:
		return &describegroups.DescribeGroupsResponse{}, true
	case 16:
		return &listgroups.ListGroupsResponse{}, true
	case 17:
		return &saslhandshake.SaslHandshakeResponse{}, true
	case 18:
		return &apiversions.ApiVersionsResponse{}, true
	case 19:
		return &createtopics.CreateTopicsResponse{}, true
	case 20:
		return &deletetopics.DeleteTopicsResponse{}, true
	case 21:
		return &deleterecords.DeleteRecordsResponse{}, true
	case 22:
		return &initproducerid.InitProducerIdResponse{}, true
	case 23:
		return &offsetforleaderepoch.OffsetForLeaderEpochResponse{}, true
	case 24:
		return &addpartitionstotxn.AddPartitionsToTxnResponse{}, true
	case 25:
		return &addoffsetstotxn.AddOffsetsToTxnResponse{}, true
	case 26:
		return &endtxn.EndTxnResponse{}, true
	case 27:
		return &writetxnmarkers.WriteTxnMarkersResponse{}, true
	case 28:
		return &txnoffsetcommit.TxnOffsetCommitResponse{}, true
	case 29:
		return &describeacls.DescribeAclsResponse{}, true
	case 30:
		return &createacls.CreateAclsResponse{}, true
	case 31:
		return &deleteacls.DeleteAclsResponse{}, true
	case 32:
		return &describeconfigs.DescribeConfigsResponse{}, true
	case 33:
		return &alterconfigs.AlterConfigsResponse{}, true
	case 34:
		return &alterreplicalogdirs.AlterReplicaLogDirsResponse{}, true
	case 35:
		return &describelogdirs.DescribeLogDirsResponse{}, true
	case 36:
		return &saslauthenticate.SaslAuthenticateResponse{}, true
	case 37:
		return &createpartitions.CreatePartitionsResponse{}, true
	case 38:
		return &createdelegationtoken.CreateDelegationTokenResponse{}, true
	case 39:
		return &renewdelegationtoken.RenewDelegationTokenResponse{}, true
	case 40:
		return &expiredelegationtoken.ExpireDelegationTokenResponse{}, true
	case 41:
		return &describedelegationtoken.DescribeDelegationTokenResponse{}, true
	case 42:
		return &deletegroups.DeleteGroupsResponse{}, true
	case 43:
		return &electleaders.ElectLeadersResponse{}, true
	case 44:
		return &incrementalalterconfigs.IncrementalAlterConfigsResponse{}, true
	case 45:
		return &alterpartitionreassignments.AlterPartitionReassignmentsResponse{}, true
	case 46:
		return &listpartitionreassignments.ListPartitionReassignmentsResponse{}, true
	case 47:
		return &offsetdelete.OffsetDeleteResponse{}, true
	case 48:
		return &describeclientquotas.DescribeClientQuotasResponse{}, true
	case 49:
		return &alterclientquotas.AlterClientQuotasResponse{}, true
	case 50:
		return &describeuserscramcredentials.DescribeUserScramCredentialsResponse{}, true
	case 51:
		return &alteruserscramcredentials.AlterUserScramCredentialsResponse{}, true
	case 52:
		return &vote.VoteResponse{}, true
	case 53:
		return &beginquorumepoch.BeginQuorumEpochResponse{}, true
	case 54:
		return &endquorumepoch.EndQuorumEpochResponse{}, true
	case 55:
		return &describequorum.DescribeQuorumResponse{}, true
	case 56:
		return &alterpartition.AlterPartitionResponse{}, true
	case 57:
		return &updatefeatures.UpdateFeaturesResponse{}, true
	case 58:
		return &envelope.EnvelopeResponse{}, true
	case 59:
		return &fetchsnapshot.FetchSnapshotResponse{}, true
	case 60:
		return &describecluster.DescribeClusterResponse{}, true
	case 61:
		return &describeproducers.DescribeProducersResponse{}, true
	case 62:
		return &brokerregistration.BrokerRegistrationResponse{}, true
	case 63:
		return &brokerheartbeat.BrokerHeartbeatResponse{}, true
	case 64:
		return &unregisterbroker.UnregisterBrokerResponse{}, true
	case 65:
		return &describetransactions.DescribeTransactionsResponse{}, true
	case 66:
		return &listtransactions.ListTransactionsResponse{}, true
	case 67:
		return &allocateproducerids.AllocateProducerIdsResponse{}, true
	case 68:
		return &consumergroupheartbeat.ConsumerGroupHeartbeatResponse{}, true
	case 69:
		return &consumergroupdescribe.ConsumerGroupDescribeResponse{}, true
	case 70:
		return &controllerregistration.ControllerRegistrationResponse{}, true
	case 71:
		return &gettelemetrysubscriptions.GetTelemetrySubscriptionsResponse{}, true
	case 72:
		return &pushtelemetry.PushTelemetryResponse{}, true
	case 73:
		return &assignreplicastodirs.AssignReplicasToDirsResponse{}, true
	case 74:
		return &listconfigresources.ListConfigResourcesResponse{}, true
	case 75:
		return &describetopicpartitions.DescribeTopicPartitionsResponse{}, true
	case 76:
		return &sharegroupheartbeat.ShareGroupHeartbeatResponse{}, true
	case 77:
		return &sharegroupdescribe.ShareGroupDescribeResponse{}, true
	case 78:
		return &sharefetch.ShareFetchResponse{}, true
	case 79:
		return &shareacknowledge.ShareAcknowledgeResponse{}, true
	case 80:
		return &addraftvoter.AddRaftVoterResponse{}, true
	case 81:
		return &removeraftvoter.RemoveRaftVoterResponse{}, true
	case 82:
		return &updateraftvoter.UpdateRaftVoterResponse{}, true
	case 83:
		return &initializesharegroupstate.InitializeShareGroupStateResponse{}, true
	case 84:
		return &readsharegroupstate.ReadShareGroupStateResponse{}, true
	case 85:
		return &writesharegroupstate.WriteShareGroupStateResponse{}, true
	case 86:
		return &deletesharegroupstate.DeleteShareGroupStateResponse{}, true
	case 87:
		return &readsharegroupstatesummary.ReadShareGroupStateSummaryResponse{}, true
	case 88:
		return &streamsgroupheartbeat.StreamsGroupHeartbeatResponse{}, true
	case 89:
		return &streamsgroupdescribe.StreamsGroupDescribeResponse{}, true
	case 90:
		return &describesharegroupoffsets.DescribeShareGroupOffsetsResponse{}, true
	case 91:
		return &altersharegroupoffsets.AlterShareGroupOffsetsResponse{}, true
	case 92:
		return &deletesharegroupoffsets.DeleteShareGroupOffsetsResponse{}, true
	default:
		return nil, false
	}
}

// Name returns the human-readable name for the given API key (for example "Metadata"),
// or "Unknown" if the API key is not recognised.
func Name(apiKey int16) string {
	switch apiKey {
	case 0:
		return "Produce"
	case 1:
		return "Fetch"
	case 2:
		return "ListOffsets"
	case 3:
		return "Metadata"
	case 8:
		return "OffsetCommit"
	case 9:
		return "OffsetFetch"
	case 10:
		return "FindCoordinator"
	case 11:
		return "JoinGroup"
	case 12:
		return "Heartbeat"
	case 13:
		return "LeaveGroup"
	case 14:
		return "SyncGroup"
	case 15:
		return "DescribeGroups"
	case 16:
		return "ListGroups"
	case 17:
		return "SaslHandshake"
	case 18:
		return "ApiVersions"
	case 19:
		return "CreateTopics"
	case 20:
		return "DeleteTopics"
	case 21:
		return "DeleteRecords"
	case 22:
		return "InitProducerId"
	case 23:
		return "OffsetForLeaderEpoch"
	case 24:
		return "AddPartitionsToTxn"
	case 25:
		return "AddOffsetsToTxn"
	case 26:
		return "EndTxn"
	case 27:
		return "WriteTxnMarkers"
	case 28:
		return "TxnOffsetCommit"
	case 29:
		return "DescribeAcls"
	case 30:
		return "CreateAcls"
	case 31:
		return "DeleteAcls"
	case 32:
		return "DescribeConfigs"
	case 33:
		return "AlterConfigs"
	case 34:
		return "AlterReplicaLogDirs"
	case 35:
		return "DescribeLogDirs"
	case 36:
		return "SaslAuthenticate"
	case 37:
		return "CreatePartitions"
	case 38:
		return "CreateDelegationToken"
	case 39:
		return "RenewDelegationToken"
	case 40:
		return "ExpireDelegationToken"
	case 41:
		return "DescribeDelegationToken"
	case 42:
		return "DeleteGroups"
	case 43:
		return "ElectLeaders"
	case 44:
		return "IncrementalAlterConfigs"
	case 45:
		return "AlterPartitionReassignments"
	case 46:
		return "ListPartitionReassignments"
	case 47:
		return "OffsetDelete"
	case 48:
		return "DescribeClientQuotas"
	case 49:
		return "AlterClientQuotas"
	case 50:
		return "DescribeUserScramCredentials"
	case 51:
		return "AlterUserScramCredentials"
	case 52:
		return "Vote"
	case 53:
		return "BeginQuorumEpoch"
	case 54:
		return "EndQuorumEpoch"
	case 55:
		return "DescribeQuorum"
	case 56:
		return "AlterPartition"
	case 57:
		return "UpdateFeatures"
	case 58:
		return "Envelope"
	case 59:
		return "FetchSnapshot"
	case 60:
		return "DescribeCluster"
	case 61:
		return "DescribeProducers"
	case 62:
		return "BrokerRegistration"
	case 63:
		return "BrokerHeartbeat"
	case 64:
		return "UnregisterBroker"
	case 65:
		return "DescribeTransactions"
	case 66:
		return "ListTransactions"
	case 67:
		return "AllocateProducerIds"
	case 68:
		return "ConsumerGroupHeartbeat"
	case 69:
		return "ConsumerGroupDescribe"
	case 70:
		return "ControllerRegistration"
	case 71:
		return "GetTelemetrySubscriptions"
	case 72:
		return "PushTelemetry"
	case 73:
		return "AssignReplicasToDirs"
	case 74:
		return "ListConfigResources"
	case 75:
		return "DescribeTopicPartitions"
	case 76:
		return "ShareGroupHeartbeat"
	case 77:
		return "ShareGroupDescribe"
	case 78:
		return "ShareFetch"
	case 79:
		return "ShareAcknowledge"
	case 80:
		return "AddRaftVoter"
	case 81:
		return "RemoveRaftVoter"
	case 82:
		return "UpdateRaftVoter"
	case 83:
		return "InitializeShareGroupState"
	case 84:
		return "ReadShareGroupState"
	case 85:
		return "WriteShareGroupState"
	case 86:
		return "DeleteShareGroupState"
	case 87:
		return "ReadShareGroupStateSummary"
	case 88:
		return "StreamsGroupHeartbeat"
	case 89:
		return "StreamsGroupDescribe"
	case 90:
		return "DescribeShareGroupOffsets"
	case 91:
		return "AlterShareGroupOffsets"
	case 92:
		return "DeleteShareGroupOffsets"
	default:
		return "Unknown"
	}
}

// VersionRange returns the minimum and maximum API versions supported by the generated
// code for the given API key. The boolean is false for unknown API keys. A proxy can use
// this to fall open - forwarding a body raw rather than attempting a decode it cannot
// round-trip - when a client and broker negotiate a version newer than this code knows.
func VersionRange(apiKey int16) (minVersion int16, maxVersion int16, ok bool) {
	switch apiKey {
	case 0:
		return 3, 13, true
	case 1:
		return 4, 18, true
	case 2:
		return 1, 11, true
	case 3:
		return 0, 13, true
	case 8:
		return 2, 10, true
	case 9:
		return 1, 10, true
	case 10:
		return 0, 6, true
	case 11:
		return 0, 9, true
	case 12:
		return 0, 4, true
	case 13:
		return 0, 5, true
	case 14:
		return 0, 5, true
	case 15:
		return 0, 6, true
	case 16:
		return 0, 5, true
	case 17:
		return 0, 1, true
	case 18:
		return 0, 4, true
	case 19:
		return 2, 7, true
	case 20:
		return 1, 6, true
	case 21:
		return 0, 2, true
	case 22:
		return 0, 6, true
	case 23:
		return 2, 4, true
	case 24:
		return 0, 5, true
	case 25:
		return 0, 4, true
	case 26:
		return 0, 5, true
	case 27:
		return 1, 2, true
	case 28:
		return 0, 5, true
	case 29:
		return 1, 3, true
	case 30:
		return 1, 3, true
	case 31:
		return 1, 3, true
	case 32:
		return 1, 4, true
	case 33:
		return 0, 2, true
	case 34:
		return 1, 2, true
	case 35:
		return 1, 5, true
	case 36:
		return 0, 2, true
	case 37:
		return 0, 3, true
	case 38:
		return 1, 3, true
	case 39:
		return 1, 2, true
	case 40:
		return 1, 2, true
	case 41:
		return 1, 3, true
	case 42:
		return 0, 2, true
	case 43:
		return 0, 2, true
	case 44:
		return 0, 1, true
	case 45:
		return 0, 1, true
	case 46:
		return 0, 0, true
	case 47:
		return 0, 0, true
	case 48:
		return 0, 1, true
	case 49:
		return 0, 1, true
	case 50:
		return 0, 0, true
	case 51:
		return 0, 0, true
	case 52:
		return 0, 2, true
	case 53:
		return 0, 1, true
	case 54:
		return 0, 1, true
	case 55:
		return 0, 2, true
	case 56:
		return 2, 3, true
	case 57:
		return 0, 2, true
	case 58:
		return 0, 0, true
	case 59:
		return 0, 1, true
	case 60:
		return 0, 2, true
	case 61:
		return 0, 0, true
	case 62:
		return 0, 4, true
	case 63:
		return 0, 2, true
	case 64:
		return 0, 0, true
	case 65:
		return 0, 0, true
	case 66:
		return 0, 2, true
	case 67:
		return 0, 0, true
	case 68:
		return 0, 1, true
	case 69:
		return 0, 1, true
	case 70:
		return 0, 0, true
	case 71:
		return 0, 0, true
	case 72:
		return 0, 0, true
	case 73:
		return 0, 0, true
	case 74:
		return 0, 1, true
	case 75:
		return 0, 0, true
	case 76:
		return 1, 1, true
	case 77:
		return 1, 1, true
	case 78:
		return 1, 2, true
	case 79:
		return 1, 2, true
	case 80:
		return 0, 1, true
	case 81:
		return 0, 0, true
	case 82:
		return 0, 0, true
	case 83:
		return 0, 0, true
	case 84:
		return 0, 0, true
	case 85:
		return 0, 1, true
	case 86:
		return 0, 0, true
	case 87:
		return 0, 1, true
	case 88:
		return 0, 0, true
	case 89:
		return 0, 0, true
	case 90:
		return 0, 1, true
	case 91:
		return 0, 0, true
	case 92:
		return 0, 0, true
	default:
		return 0, 0, false
	}
}
