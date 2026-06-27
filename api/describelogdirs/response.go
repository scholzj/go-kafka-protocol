package describelogdirs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeLogDirsResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32                            // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode       int16                            // The error code, or 0 if there was no error. (versions: 3+)
	Results         *[]DescribeLogDirsResponseResult // The log directories. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeLogDirsResponseResult struct {
	ErrorCode       int16                                 // The error code, or 0 if there was no error. (versions: 0+)
	LogDir          *string                               // The absolute log directory path. (versions: 0+)
	Topics          *[]DescribeLogDirsResponseResultTopic // The topics. (versions: 0+)
	TotalBytes      int64                                 // The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage. (versions: 4+)
	UsableBytes     int64                                 // The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage. (versions: 4+)
	IsCordoned      bool                                  // True if this log directory is cordoned. (versions: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeLogDirsResponseResultTopic struct {
	Name            *string                                        // The topic name. (versions: 0+)
	Partitions      *[]DescribeLogDirsResponseResultTopicPartition // The partitions. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeLogDirsResponseResultTopicPartition struct {
	PartitionIndex  int32 // The partition index. (versions: 0+)
	PartitionSize   int64 // The size of the log segments in this partition in bytes. (versions: 0+)
	OffsetLag       int64 // The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition). (versions: 0+)
	IsFutureKey     bool  // True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *DescribeLogDirsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 3+)
	if res.ApiVersion >= 3 {
		if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
			return err
		}
	}

	// Results (versions: 0+)
	if res.Results == nil {
		return fmt.Errorf("DescribeLogDirsResponse.Results must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.resultsEncoder, res.Results); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.resultsEncoder, *res.Results); err != nil {
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
func (res *DescribeLogDirsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("DescribeLogDirsResponse.Read: response or its body is nil")
	}

	*res = DescribeLogDirsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 3+)
	if res.ApiVersion >= 3 {
		errorcode, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		res.ErrorCode = errorcode
	}

	// Results (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		results, err := protocol.ReadCompactArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
	} else {
		results, err := protocol.ReadArray(r, res.resultsDecoder)
		if err != nil {
			return err
		}
		res.Results = &results
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

func (res *DescribeLogDirsResponse) resultsEncoder(w io.Writer, value DescribeLogDirsResponseResult) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, value.ErrorCode); err != nil {
		return err
	}

	// LogDir (versions: 0+)
	if value.LogDir == nil {
		return fmt.Errorf("DescribeLogDirsResponseResult.LogDir must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.LogDir); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.LogDir); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("DescribeLogDirsResponseResult.Topics must not be nil in version %d", res.ApiVersion)
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

	// TotalBytes (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.TotalBytes); err != nil {
			return err
		}
	}

	// UsableBytes (versions: 4+)
	if res.ApiVersion >= 4 {
		if err := protocol.WriteInt64(w, value.UsableBytes); err != nil {
			return err
		}
	}

	// IsCordoned (versions: 5+)
	if res.ApiVersion >= 5 {
		if err := protocol.WriteBool(w, value.IsCordoned); err != nil {
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

func (res *DescribeLogDirsResponse) resultsDecoder(r io.Reader) (DescribeLogDirsResponseResult, error) {
	describelogdirsresponseresult := DescribeLogDirsResponseResult{}

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	describelogdirsresponseresult.TotalBytes = -1
	describelogdirsresponseresult.UsableBytes = -1

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return describelogdirsresponseresult, err
	}
	describelogdirsresponseresult.ErrorCode = errorcode

	// LogDir (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		logdir, err := protocol.ReadCompactString(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.LogDir = &logdir
	} else {
		logdir, err := protocol.ReadString(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.LogDir = &logdir
	}

	// Topics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, res.topicsDecoder)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, res.topicsDecoder)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.Topics = &topics
	}

	// TotalBytes (versions: 4+)
	if res.ApiVersion >= 4 {
		totalbytes, err := protocol.ReadInt64(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.TotalBytes = totalbytes
	}

	// UsableBytes (versions: 4+)
	if res.ApiVersion >= 4 {
		usablebytes, err := protocol.ReadInt64(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.UsableBytes = usablebytes
	}

	// IsCordoned (versions: 5+)
	if res.ApiVersion >= 5 {
		iscordoned, err := protocol.ReadBool(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.IsCordoned = iscordoned
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describelogdirsresponseresult, err
		}
		describelogdirsresponseresult.rawTaggedFields = &rawTaggedFields
	}

	return describelogdirsresponseresult, nil
}

func (res *DescribeLogDirsResponse) topicsEncoder(w io.Writer, value DescribeLogDirsResponseResultTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeLogDirsResponseResultTopic.Name must not be nil in version %d", res.ApiVersion)
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
	if value.Partitions == nil {
		return fmt.Errorf("DescribeLogDirsResponseResultTopic.Partitions must not be nil in version %d", res.ApiVersion)
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

func (res *DescribeLogDirsResponse) topicsDecoder(r io.Reader) (DescribeLogDirsResponseResultTopic, error) {
	describelogdirsresponseresulttopic := DescribeLogDirsResponseResultTopic{}

	// Name (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describelogdirsresponseresulttopic, err
		}
		describelogdirsresponseresulttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describelogdirsresponseresulttopic, err
		}
		describelogdirsresponseresulttopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, res.partitionsDecoder)
		if err != nil {
			return describelogdirsresponseresulttopic, err
		}
		describelogdirsresponseresulttopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, res.partitionsDecoder)
		if err != nil {
			return describelogdirsresponseresulttopic, err
		}
		describelogdirsresponseresulttopic.Partitions = &partitions
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describelogdirsresponseresulttopic, err
		}
		describelogdirsresponseresulttopic.rawTaggedFields = &rawTaggedFields
	}

	return describelogdirsresponseresulttopic, nil
}

func (res *DescribeLogDirsResponse) partitionsEncoder(w io.Writer, value DescribeLogDirsResponseResultTopicPartition) error {
	// PartitionIndex (versions: 0+)
	if err := protocol.WriteInt32(w, value.PartitionIndex); err != nil {
		return err
	}

	// PartitionSize (versions: 0+)
	if err := protocol.WriteInt64(w, value.PartitionSize); err != nil {
		return err
	}

	// OffsetLag (versions: 0+)
	if err := protocol.WriteInt64(w, value.OffsetLag); err != nil {
		return err
	}

	// IsFutureKey (versions: 0+)
	if err := protocol.WriteBool(w, value.IsFutureKey); err != nil {
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

func (res *DescribeLogDirsResponse) partitionsDecoder(r io.Reader) (DescribeLogDirsResponseResultTopicPartition, error) {
	describelogdirsresponseresulttopicpartition := DescribeLogDirsResponseResultTopicPartition{}

	// PartitionIndex (versions: 0+)
	partitionindex, err := protocol.ReadInt32(r)
	if err != nil {
		return describelogdirsresponseresulttopicpartition, err
	}
	describelogdirsresponseresulttopicpartition.PartitionIndex = partitionindex

	// PartitionSize (versions: 0+)
	partitionsize, err := protocol.ReadInt64(r)
	if err != nil {
		return describelogdirsresponseresulttopicpartition, err
	}
	describelogdirsresponseresulttopicpartition.PartitionSize = partitionsize

	// OffsetLag (versions: 0+)
	offsetlag, err := protocol.ReadInt64(r)
	if err != nil {
		return describelogdirsresponseresulttopicpartition, err
	}
	describelogdirsresponseresulttopicpartition.OffsetLag = offsetlag

	// IsFutureKey (versions: 0+)
	isfuturekey, err := protocol.ReadBool(r)
	if err != nil {
		return describelogdirsresponseresulttopicpartition, err
	}
	describelogdirsresponseresulttopicpartition.IsFutureKey = isfuturekey

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describelogdirsresponseresulttopicpartition, err
		}
		describelogdirsresponseresulttopicpartition.rawTaggedFields = &rawTaggedFields
	}

	return describelogdirsresponseresulttopicpartition, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeLogDirsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeLogDirsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.Results != nil {
		fmt.Fprintf(w, "        Results:\n")
		for _, results := range *res.Results {
			fmt.Fprintf(w, "%s", results.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Results: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeLogDirsResponseResult) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            ErrorCode: %v\n", value.ErrorCode)

	if value.LogDir != nil {
		fmt.Fprintf(w, "            LogDir: %v\n", *value.LogDir)
	} else {
		fmt.Fprintf(w, "            LogDir: nil\n")
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

	fmt.Fprintf(w, "            TotalBytes: %v\n", value.TotalBytes)
	fmt.Fprintf(w, "            UsableBytes: %v\n", value.UsableBytes)
	fmt.Fprintf(w, "            IsCordoned: %v\n", value.IsCordoned)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeLogDirsResponseResultTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

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
func (value *DescribeLogDirsResponseResultTopicPartition) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "                    PartitionIndex: %v\n", value.PartitionIndex)
	fmt.Fprintf(w, "                    PartitionSize: %v\n", value.PartitionSize)
	fmt.Fprintf(w, "                    OffsetLag: %v\n", value.OffsetLag)
	fmt.Fprintf(w, "                    IsFutureKey: %v\n", value.IsFutureKey)

	return w.String()
}
