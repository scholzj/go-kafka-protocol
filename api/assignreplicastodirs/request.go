package assignreplicastodirs

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AssignReplicasToDirsRequest struct {
	ApiVersion      int16
	BrokerId        int32                                    // The ID of the requesting broker. (versions: 0+)
	BrokerEpoch     int64                                    // The epoch of the requesting broker. (versions: 0+)
	Directories     *[]AssignReplicasToDirsRequestDirectorie // The directories to which replicas should be assigned. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsRequestDirectorie struct {
	Id              uuid.UUID                                     // The ID of the directory. (versions: 0+)
	Topics          *[]AssignReplicasToDirsRequestDirectorieTopic // The topics assigned to the directory. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsRequestDirectorieTopic struct {
	TopicId         uuid.UUID                                              // The ID of the assigned topic. (versions: 0+)
	Partitions      *[]AssignReplicasToDirsRequestDirectorieTopicPartition // The partitions assigned to the directory. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsRequestDirectorieTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *AssignReplicasToDirsRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
		return err
	}

	// BrokerEpoch (versions: 0+)
	if err := protocol.WriteInt64(w, req.BrokerEpoch); err != nil {
		return err
	}

	// Directories (versions: 0+)
	if req.Directories == nil {
		return fmt.Errorf("AssignReplicasToDirsRequest.Directories must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.directoriesEncoder, req.Directories); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.directoriesEncoder, *req.Directories); err != nil {
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
func (req *AssignReplicasToDirsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AssignReplicasToDirsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

	// BrokerEpoch (versions: 0+)
	brokerepoch, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.BrokerEpoch = brokerepoch

	// Directories (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		directories, err := protocol.ReadNullableCompactArray(r, req.directoriesDecoder)
		if err != nil {
			return err
		}
		req.Directories = directories
	} else {
		directories, err := protocol.ReadArray(r, req.directoriesDecoder)
		if err != nil {
			return err
		}
		req.Directories = &directories
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

func (req *AssignReplicasToDirsRequest) directoriesEncoder(w io.Writer, value AssignReplicasToDirsRequestDirectorie) error {
	// Id (versions: 0+)
	if err := protocol.WriteUUID(w, value.Id); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("AssignReplicasToDirsRequestDirectorie.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *value.Topics); err != nil {
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

func (req *AssignReplicasToDirsRequest) directoriesDecoder(r io.Reader) (AssignReplicasToDirsRequestDirectorie, error) {
	assignreplicastodirsrequestdirectorie := AssignReplicasToDirsRequestDirectorie{}

	// Id (versions: 0+)
	id, err := protocol.ReadUUID(r)
	if err != nil {
		return assignreplicastodirsrequestdirectorie, err
	}
	assignreplicastodirsrequestdirectorie.Id = id

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return assignreplicastodirsrequestdirectorie, err
		}
		assignreplicastodirsrequestdirectorie.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return assignreplicastodirsrequestdirectorie, err
		}
		assignreplicastodirsrequestdirectorie.Topics = &topics
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsrequestdirectorie, err
		}
		assignreplicastodirsrequestdirectorie.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsrequestdirectorie, nil
}

func (req *AssignReplicasToDirsRequest) topicsEncoder(w io.Writer, value AssignReplicasToDirsRequestDirectorieTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AssignReplicasToDirsRequestDirectorieTopic.Partitions must not be nil in version %d", req.ApiVersion)
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

func (req *AssignReplicasToDirsRequest) topicsDecoder(r io.Reader) (AssignReplicasToDirsRequestDirectorieTopic, error) {
	assignreplicastodirsrequestdirectorietopic := AssignReplicasToDirsRequestDirectorieTopic{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return assignreplicastodirsrequestdirectorietopic, err
	}
	assignreplicastodirsrequestdirectorietopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, req.partitionsDecoder)
		if err != nil {
			return assignreplicastodirsrequestdirectorietopic, err
		}
		assignreplicastodirsrequestdirectorietopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, req.partitionsDecoder)
		if err != nil {
			return assignreplicastodirsrequestdirectorietopic, err
		}
		assignreplicastodirsrequestdirectorietopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsrequestdirectorietopic, err
		}
		assignreplicastodirsrequestdirectorietopic.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsrequestdirectorietopic, nil
}

func (req *AssignReplicasToDirsRequest) partitionsEncoder(w io.Writer, value AssignReplicasToDirsRequestDirectorieTopicPartition) error {
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

func (req *AssignReplicasToDirsRequest) partitionsDecoder(r io.Reader) (AssignReplicasToDirsRequestDirectorieTopicPartition, error) {
	assignreplicastodirsrequestdirectorietopicpartition := AssignReplicasToDirsRequestDirectorieTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return assignreplicastodirsrequestdirectorietopicpartition, err
	}
	assignreplicastodirsrequestdirectorietopicpartition.PartitionIndex = partitionindex

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsrequestdirectorietopicpartition, err
		}
		assignreplicastodirsrequestdirectorietopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsrequestdirectorietopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AssignReplicasToDirsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AssignReplicasToDirsRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)
	fmt.Fprintf(w, "        BrokerEpoch: %v\n", req.BrokerEpoch)

	if req.Directories != nil {
		fmt.Fprintf(w, "        Directories:\n")
		for _, directories := range *req.Directories {
			fmt.Fprintf(w, "%s", directories.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Directories: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AssignReplicasToDirsRequestDirectorie) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            Id: %v\n", value.Id)

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
func (value *AssignReplicasToDirsRequestDirectorieTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                TopicId: %v\n", value.TopicId)

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions:\n")
		for _, partitions := range *value.Partitions {
			fmt.Fprintf(w, "%s", partitions.PrettyPrint())
			fmt.Fprintf(w, "                    ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AssignReplicasToDirsRequestDirectorieTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)

	return w.String()
}
