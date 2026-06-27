package listpartitionreassignments

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListPartitionReassignmentsRequest struct {
	ApiVersion      int16
	TimeoutMs       int32                                     // The time in ms to wait for the request to complete. (versions: 0+)
	Topics          *[]ListPartitionReassignmentsRequestTopic // The topics to list partition reassignments for, or null to list everything. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type ListPartitionReassignmentsRequestTopic struct {
	Name             *string  // The topic name. (versions: 0+)
	PartitionIndexes *[]int32 // The partitions to list partition reassignments for. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ListPartitionReassignmentsRequest) Write(w io.Writer) error {
	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, req.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.topicsEncoder, req.Topics); err != nil {
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
func (req *ListPartitionReassignmentsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ListPartitionReassignmentsRequest.Read: request or its body is nil")
	}

	*req = ListPartitionReassignmentsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.TimeoutMs = 60000

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadNullableCompactArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
	} else {
		topics, err := protocol.ReadNullableArray(r, req.topicsDecoder)
		if err != nil {
			return err
		}
		req.Topics = topics
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

func (req *ListPartitionReassignmentsRequest) topicsEncoder(w io.Writer, value ListPartitionReassignmentsRequestTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ListPartitionReassignmentsRequestTopic.Name must not be nil in version %d", req.ApiVersion)
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

	// PartitionIndexes (versions: 0+)
	if value.PartitionIndexes == nil {
		return fmt.Errorf("ListPartitionReassignmentsRequestTopic.PartitionIndexes must not be nil in version %d", req.ApiVersion)
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

func (req *ListPartitionReassignmentsRequest) topicsDecoder(r io.Reader) (ListPartitionReassignmentsRequestTopic, error) {
	listpartitionreassignmentsrequesttopic := ListPartitionReassignmentsRequestTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return listpartitionreassignmentsrequesttopic, err
		}
		listpartitionreassignmentsrequesttopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return listpartitionreassignmentsrequesttopic, err
		}
		listpartitionreassignmentsrequesttopic.Name = &name
	}

	// PartitionIndexes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitionindexes, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsrequesttopic, err
		}
		listpartitionreassignmentsrequesttopic.PartitionIndexes = &partitionindexes
	} else {
		partitionindexes, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return listpartitionreassignmentsrequesttopic, err
		}
		listpartitionreassignmentsrequesttopic.PartitionIndexes = &partitionindexes
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return listpartitionreassignmentsrequesttopic, err
		}
		listpartitionreassignmentsrequesttopic.rawTaggedFields = &rawTaggedFields
	}

	return listpartitionreassignmentsrequesttopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ListPartitionReassignmentsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ListPartitionReassignmentsRequest:\n")
	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)

	if req.Topics != nil {
		fmt.Fprintf(w, "        Topics:\n")
		for _, topics := range *req.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ListPartitionReassignmentsRequestTopic) PrettyPrint() string {
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
