package deletegroups

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DeleteGroupsRequest struct {
	ApiVersion      int16
	GroupsNames     *[]string // The group names to delete. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DeleteGroupsRequest) Write(w io.Writer) error {
	// GroupsNames (versions: 0+)
	if req.GroupsNames == nil {
		return fmt.Errorf("DeleteGroupsRequest.GroupsNames must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.GroupsNames); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *req.GroupsNames); err != nil {
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
func (req *DeleteGroupsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DeleteGroupsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// GroupsNames (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groupsnames, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		req.GroupsNames = groupsnames
	} else {
		groupsnames, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		req.GroupsNames = &groupsnames
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

//goland:noinspection GoUnhandledErrorResult
func (req *DeleteGroupsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DeleteGroupsRequest:\n")

	if req.GroupsNames != nil {
		fmt.Fprintf(w, "        GroupsNames: %v\n", *req.GroupsNames)
	} else {
		fmt.Fprintf(w, "        GroupsNames: nil\n")
	}

	return w.String()
}
