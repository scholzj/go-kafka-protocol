package listgroups

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ListGroupsRequest struct {
	ApiVersion      int16
	StatesFilter    *[]string // The states of the groups we want to list. If empty, all groups are returned with their state. (versions: 4+)
	TypesFilter     *[]string // The types of the groups we want to list. If empty, all groups are returned with their type. (versions: 5+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *ListGroupsRequest) Write(w io.Writer) error {
	// StatesFilter (versions: 4+)
	if req.ApiVersion >= 4 {
		if req.StatesFilter == nil {
			return fmt.Errorf("ListGroupsRequest.StatesFilter must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.StatesFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteString, *req.StatesFilter); err != nil {
				return err
			}
		}
	}

	// TypesFilter (versions: 5+)
	if req.ApiVersion >= 5 {
		if req.TypesFilter == nil {
			return fmt.Errorf("ListGroupsRequest.TypesFilter must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.TypesFilter); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteString, *req.TypesFilter); err != nil {
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
func (req *ListGroupsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ListGroupsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// StatesFilter (versions: 4+)
	if req.ApiVersion >= 4 {
		if isRequestFlexible(req.ApiVersion) {
			statesfilter, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
			if err != nil {
				return err
			}
			req.StatesFilter = statesfilter
		} else {
			statesfilter, err := protocol.ReadArray(r, protocol.ReadString)
			if err != nil {
				return err
			}
			req.StatesFilter = &statesfilter
		}
	}

	// TypesFilter (versions: 5+)
	if req.ApiVersion >= 5 {
		if isRequestFlexible(req.ApiVersion) {
			typesfilter, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
			if err != nil {
				return err
			}
			req.TypesFilter = typesfilter
		} else {
			typesfilter, err := protocol.ReadArray(r, protocol.ReadString)
			if err != nil {
				return err
			}
			req.TypesFilter = &typesfilter
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

//goland:noinspection GoUnhandledErrorResult
func (req *ListGroupsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ListGroupsRequest:\n")

	if req.StatesFilter != nil {
		fmt.Fprintf(w, "        StatesFilter: %v\n", *req.StatesFilter)
	} else {
		fmt.Fprintf(w, "        StatesFilter: nil\n")
	}

	if req.TypesFilter != nil {
		fmt.Fprintf(w, "        TypesFilter: %v\n", *req.TypesFilter)
	} else {
		fmt.Fprintf(w, "        TypesFilter: nil\n")
	}

	return w.String()
}
