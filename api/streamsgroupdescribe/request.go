package streamsgroupdescribe

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type StreamsGroupDescribeRequest struct {
	ApiVersion                  int16
	GroupIds                    *[]string // The ids of the groups to describe (versions: 0+)
	IncludeAuthorizedOperations bool      // Whether to include authorized operations. (versions: 0+)
	rawTaggedFields             *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *StreamsGroupDescribeRequest) Write(w io.Writer) error {
	// GroupIds (versions: 0+)
	if req.GroupIds == nil {
		return fmt.Errorf("StreamsGroupDescribeRequest.GroupIds must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.GroupIds); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *req.GroupIds); err != nil {
			return err
		}
	}

	// IncludeAuthorizedOperations (versions: 0+)
	if err := protocol.WriteBool(w, req.IncludeAuthorizedOperations); err != nil {
		return err
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
func (req *StreamsGroupDescribeRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("StreamsGroupDescribeRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupIds (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupids, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		req.GroupIds = groupids
	} else {
		groupids, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		req.GroupIds = &groupids
	}

	// IncludeAuthorizedOperations (versions: 0+)
	includeauthorizedoperations, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.IncludeAuthorizedOperations = includeauthorizedoperations

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

//goland:noinspection GoUnhandledErrorResult
func (req *StreamsGroupDescribeRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> StreamsGroupDescribeRequest:\n")

	if req.GroupIds != nil {
		fmt.Fprintf(w, "        GroupIds: %v\n", *req.GroupIds)
	} else {
		fmt.Fprintf(w, "        GroupIds: nil\n")
	}

	fmt.Fprintf(w, "        IncludeAuthorizedOperations: %v\n", req.IncludeAuthorizedOperations)

	return w.String()
}
