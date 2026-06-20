package describegroups

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeGroupsRequest struct {
	ApiVersion                  int16
	Groups                      *[]string // The names of the groups to describe. (versions: 0+)
	IncludeAuthorizedOperations bool      // Whether to include authorized operations. (versions: 3+)
	rawTaggedFields             *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 5
}

func (req *DescribeGroupsRequest) Write(w io.Writer) error {
	// Groups (versions: 0+)
	if req.Groups == nil {
		return fmt.Errorf("DescribeGroupsRequest.Groups must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, req.Groups); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *req.Groups); err != nil {
			return err
		}
	}

	// IncludeAuthorizedOperations (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteBool(w, req.IncludeAuthorizedOperations); err != nil {
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
func (req *DescribeGroupsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeGroupsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Groups (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		groups, err := protocol.ReadNullableCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		req.Groups = groups
	} else {
		groups, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		req.Groups = &groups
	}

	// IncludeAuthorizedOperations (versions: 3+)
	if req.ApiVersion >= 3 {
		includeauthorizedoperations, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeAuthorizedOperations = includeauthorizedoperations
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
func (req *DescribeGroupsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeGroupsRequest:\n")

	if req.Groups != nil {
		fmt.Fprintf(w, "        Groups: %v\n", *req.Groups)
	} else {
		fmt.Fprintf(w, "        Groups: nil\n")
	}

	fmt.Fprintf(w, "        IncludeAuthorizedOperations: %v\n", req.IncludeAuthorizedOperations)

	return w.String()
}
