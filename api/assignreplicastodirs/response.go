package assignreplicastodirs

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AssignReplicasToDirsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                                     // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                                     // The top level response error code. (versions: 0+)
	Directories     *[]AssignReplicasToDirsResponseDirectorie // The list of directories and their assigned partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsResponseDirectorie struct {
	Id              uuid.UUID                                      // The ID of the directory. (versions: 0+)
	Topics          *[]AssignReplicasToDirsResponseDirectorieTopic // The list of topics and their assigned partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsResponseDirectorieTopic struct {
	TopicId         uuid.UUID                                               // The ID of the assigned topic. (versions: 0+)
	Partitions      *[]AssignReplicasToDirsResponseDirectorieTopicPartition // The list of assigned partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AssignReplicasToDirsResponseDirectorieTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	ErrorCode       int16 // The partition level error code. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *AssignReplicasToDirsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// Directories (versions: 0+)
	if res.Directories == nil {
		return fmt.Errorf("AssignReplicasToDirsResponse.Directories must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.directoriesEncoder, res.Directories); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.directoriesEncoder, *res.Directories); err != nil {
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
func (res *AssignReplicasToDirsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("AssignReplicasToDirsResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// Directories (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		directories, err := protocol.ReadNullableCompactArray(r, res.directoriesDecoder)
		if err != nil {
			return err
		}
		res.Directories = directories
	} else {
		directories, err := protocol.ReadArray(r, res.directoriesDecoder)
		if err != nil {
			return err
		}
		res.Directories = &directories
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

func (res *AssignReplicasToDirsResponse) directoriesEncoder(w io.Writer, value AssignReplicasToDirsResponseDirectorie) error {
	// Id (versions: 0+)
	if err := protocol.WriteUUID(w, value.Id); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("AssignReplicasToDirsResponseDirectorie.Topics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.topicsEncoder, *value.Topics); err != nil {
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

func (res *AssignReplicasToDirsResponse) directoriesDecoder(r io.Reader) (AssignReplicasToDirsResponseDirectorie, error) {
	assignreplicastodirsresponsedirectorie := AssignReplicasToDirsResponseDirectorie{}

	// Id (versions: 0+)
	id, err := protocol.ReadUUID(r)
	if err != nil {
		return assignreplicastodirsresponsedirectorie, err
	}
	assignreplicastodirsresponsedirectorie.Id = id

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, res.topicsDecoder)
		if err != nil {
			return assignreplicastodirsresponsedirectorie, err
		}
		assignreplicastodirsresponsedirectorie.Topics = topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return assignreplicastodirsresponsedirectorie, err
		}
		assignreplicastodirsresponsedirectorie.Topics = &topics
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsresponsedirectorie, err
		}
		assignreplicastodirsresponsedirectorie.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsresponsedirectorie, nil
}

func (res *AssignReplicasToDirsResponse) topicsEncoder(w io.Writer, value AssignReplicasToDirsResponseDirectorieTopic) error {
	// TopicId (versions: 0+)
	if err := protocol.WriteUUID(w, value.TopicId); err != nil {
		return err
	}

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AssignReplicasToDirsResponseDirectorieTopic.Partitions must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.partitionsEncoder, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.partitionsEncoder, *value.Partitions); err != nil {
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

func (res *AssignReplicasToDirsResponse) topicsDecoder(r io.Reader) (AssignReplicasToDirsResponseDirectorieTopic, error) {
	assignreplicastodirsresponsedirectorietopic := AssignReplicasToDirsResponseDirectorieTopic{}

	// TopicId (versions: 0+)
	topicid, err := protocol.ReadUUID(r)
	if err != nil {
		return assignreplicastodirsresponsedirectorietopic, err
	}
	assignreplicastodirsresponsedirectorietopic.TopicId = topicid

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadNullableCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return assignreplicastodirsresponsedirectorietopic, err
		}
		assignreplicastodirsresponsedirectorietopic.Partitions = partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return assignreplicastodirsresponsedirectorietopic, err
		}
		assignreplicastodirsresponsedirectorietopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsresponsedirectorietopic, err
		}
		assignreplicastodirsresponsedirectorietopic.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsresponsedirectorietopic, nil
}

func (res *AssignReplicasToDirsResponse) partitionsEncoder(w io.Writer, value AssignReplicasToDirsResponseDirectorieTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
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

func (res *AssignReplicasToDirsResponse) partitionsDecoder(r io.Reader) (AssignReplicasToDirsResponseDirectorieTopicPartition, error) {
	assignreplicastodirsresponsedirectorietopicpartition := AssignReplicasToDirsResponseDirectorieTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return assignreplicastodirsresponsedirectorietopicpartition, err
	}
	assignreplicastodirsresponsedirectorietopicpartition.PartitionIndex = partitionindex

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return assignreplicastodirsresponsedirectorietopicpartition, err
	}
	assignreplicastodirsresponsedirectorietopicpartition.ErrorCode = errorcode

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return assignreplicastodirsresponsedirectorietopicpartition, err
		}
		assignreplicastodirsresponsedirectorietopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return assignreplicastodirsresponsedirectorietopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *AssignReplicasToDirsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- AssignReplicasToDirsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Directories != nil {
		fmt.Fprintf(w, "        Directories:\n")
		for _, directories := range *res.Directories {
			fmt.Fprintf(w, "%s", directories.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Directories: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AssignReplicasToDirsResponseDirectorie) PrettyPrint() string {
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
func (value *AssignReplicasToDirsResponseDirectorieTopic) PrettyPrint() string {
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
func (value *AssignReplicasToDirsResponseDirectorieTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    ErrorCode: %v\n", value.ErrorCode)

	return w.String()
}
