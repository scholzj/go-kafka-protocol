package apis

func RequestHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case 0: // Produce
		if apiVersion >= 9 {
			return 2
		} else {
			return 1
		}
	case 1: // Fetch
		if apiVersion >= 12 {
			return 2
		} else {
			return 1
		}
	case 2: // ListOffsets
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case 3: // Metadata
		if apiVersion >= 9 {
			return 2
		} else {
			return 1
		}
	case 8: // OffsetCommit
		if apiVersion >= 8 {
			return 2
		} else {
			return 1
		}
	case 9: // OffsetFetch
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case 10: // FindCoordinator
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 11: // JoinGroup
		if apiVersion >= 6 {
			return 2
		} else {
			return 1
		}
	case 12: // Heartbeat
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 13: // LeaveGroup
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 14: // SyncGroup
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 15: // DescribeGroups
		if apiVersion >= 5 {
			return 2
		} else {
			return 1
		}
	case 16: // ListGroups
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 17: // SaslHandshake
		return 1
	case 18: // ApiVersions
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 19: // CreateTopics
		if apiVersion >= 5 {
			return 2
		} else {
			return 1
		}
	case 20: // DeleteTopics
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 21: // DeleteRecords
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 22: // InitProducerId
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 23: // OffsetForLeaderEpoch
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 24: // AddPartitionsToTxn
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 25: // AddOffsetsToTxn
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 26: // EndTxn
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 27: // WriteTxnMarkers
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 28: // TxnOffsetCommit
		if apiVersion >= 3 {
			return 2
		} else {
			return 1
		}
	case 29: // DescribeAcls
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 30: // CreateAcls
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 31: // DeleteAcls
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 32: // DescribeConfigs
		if apiVersion >= 4 {
			return 2
		} else {
			return 1
		}
	case 33: // AlterConfigs
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 34: // AlterReplicaLogDirs
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 35: // DescribeLogDirs
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 36: // SaslAuthenticate
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 37: // CreatePartitions
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 38: // CreateDelegationToken
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 39: // RenewDelegationToken
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 40: // ExpireDelegationToken
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 41: // DescribeDelegationToken
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 42: // DeleteGroups
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 43: // ElectLeaders
		if apiVersion >= 2 {
			return 2
		} else {
			return 1
		}
	case 44: // IncrementalAlterConfigs
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 45: // AlterPartitionReassignments
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 46: // ListPartitionReassignments
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 47: // OffsetDelete
		return 1
	case 48: // DescribeClientQuotas
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 49: // AlterClientQuotas
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 50: // DescribeUserScramCredentials
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 51: // AlterUserScramCredentials
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 52: // Vote
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 53: // BeginQuorumEpoch
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 54: // EndQuorumEpoch
		if apiVersion >= 1 {
			return 2
		} else {
			return 1
		}
	case 55: // DescribeQuorum
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 56: // AlterPartition
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 57: // UpdateFeatures
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 58: // Envelope
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 59: // FetchSnapshot
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 60: // DescribeCluster
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 61: // DescribeProducers
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 62: // BrokerRegistration
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 63: // BrokerHeartbeat
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 64: // UnregisterBroker
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 65: // DescribeTransactions
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 66: // ListTransactions
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 67: // AllocateProducerIds
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 68: // ConsumerGroupHeartbeat
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 69: // ConsumerGroupDescribe
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 70: // ControllerRegistration
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 71: // GetTelemetrySubscriptions
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 72: // PushTelemetry
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 73: // AssignReplicasToDirs
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 74: // ListConfigResources
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 75: // DescribeTopicPartitions
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 76: // ShareGroupHeartbeat
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 77: // ShareGroupDescribe
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 78: // ShareFetch
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 79: // ShareAcknowledge
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 80: // AddRaftVoter
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 81: // RemoveRaftVoter
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 82: // UpdateRaftVoter
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 83: // InitializeShareGroupState
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 84: // ReadShareGroupState
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 85: // WriteShareGroupState
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 86: // DeleteShareGroupState
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 87: // ReadShareGroupStateSummary
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 88: // StreamsGroupHeartbeat
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 89: // StreamsGroupDescribe
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 90: // DescribeShareGroupOffsets
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 91: // AlterShareGroupOffsets
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	case 92: // DeleteShareGroupOffsets
		if apiVersion >= 0 {
			return 2
		} else {
			return 1
		}
	default:
		return 1
	}
}

func ResponseHeaderVersion(apiKey int16, apiVersion int16) int16 {
	switch apiKey {
	case 0: // Produce
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 1: // Fetch
		if apiVersion >= 12 {
			return 1
		} else {
			return 0
		}
	case 2: // ListOffsets
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 3: // Metadata
		if apiVersion >= 9 {
			return 1
		} else {
			return 0
		}
	case 8: // OffsetCommit
		if apiVersion >= 8 {
			return 1
		} else {
			return 0
		}
	case 9: // OffsetFetch
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 10: // FindCoordinator
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 11: // JoinGroup
		if apiVersion >= 6 {
			return 1
		} else {
			return 0
		}
	case 12: // Heartbeat
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 13: // LeaveGroup
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 14: // SyncGroup
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 15: // DescribeGroups
		if apiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 16: // ListGroups
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 17: // SaslHandshake
		return 0
	case 18: // ApiVersions
		// Always uses response header 0
		return 0
	case 19: // CreateTopics
		if apiVersion >= 5 {
			return 1
		} else {
			return 0
		}
	case 20: // DeleteTopics
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 21: // DeleteRecords
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 22: // InitProducerId
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 23: // OffsetForLeaderEpoch
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 24: // AddPartitionsToTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 25: // AddOffsetsToTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 26: // EndTxn
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 27: // WriteTxnMarkers
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 28: // TxnOffsetCommit
		if apiVersion >= 3 {
			return 1
		} else {
			return 0
		}
	case 29: // DescribeAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 30: // CreateAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 31: // DeleteAcls
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 32: // DescribeConfigs
		if apiVersion >= 4 {
			return 1
		} else {
			return 0
		}
	case 33: // AlterConfigs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 34: // AlterReplicaLogDirs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 35: // DescribeLogDirs
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 36: // SaslAuthenticate
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 37: // CreatePartitions
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 38: // CreateDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 39: // RenewDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 40: // ExpireDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 41: // DescribeDelegationToken
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 42: // DeleteGroups
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 43: // ElectLeaders
		if apiVersion >= 2 {
			return 1
		} else {
			return 0
		}
	case 44: // IncrementalAlterConfigs
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 45: // AlterPartitionReassignments
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 46: // ListPartitionReassignments
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 47: // OffsetDelete
		return 0
	case 48: // DescribeClientQuotas
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 49: // AlterClientQuotas
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 50: // DescribeUserScramCredentials
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 51: // AlterUserScramCredentials
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 52: // Vote
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 53: // BeginQuorumEpoch
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 54: // EndQuorumEpoch
		if apiVersion >= 1 {
			return 1
		} else {
			return 0
		}
	case 55: // DescribeQuorum
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 56: // AlterPartition
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 57: // UpdateFeatures
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 58: // Envelope
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 59: // FetchSnapshot
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 60: // DescribeCluster
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 61: // DescribeProducers
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 62: // BrokerRegistration
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 63: // BrokerHeartbeat
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 64: // UnregisterBroker
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 65: // DescribeTransactions
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 66: // ListTransactions
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 67: // AllocateProducerIds
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 68: // ConsumerGroupHeartbeat
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 69: // ConsumerGroupDescribe
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 70: // ControllerRegistration
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 71: // GetTelemetrySubscriptions
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 72: // PushTelemetry
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 73: // AssignReplicasToDirs
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 74: // ListConfigResources
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 75: // DescribeTopicPartitions
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 76: // ShareGroupHeartbeat
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 77: // ShareGroupDescribe
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 78: // ShareFetch
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 79: // ShareAcknowledge
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 80: // AddRaftVoter
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 81: // RemoveRaftVoter
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 82: // UpdateRaftVoter
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 83: // InitializeShareGroupState
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 84: // ReadShareGroupState
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 85: // WriteShareGroupState
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 86: // DeleteShareGroupState
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 87: // ReadShareGroupStateSummary
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 88: // StreamsGroupHeartbeat
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 89: // StreamsGroupDescribe
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 90: // DescribeShareGroupOffsets
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 91: // AlterShareGroupOffsets
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	case 92: // DeleteShareGroupOffsets
		if apiVersion >= 0 {
			return 1
		} else {
			return 0
		}
	default:
		return 0
	}
}
