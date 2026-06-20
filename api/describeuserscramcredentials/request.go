package describeuserscramcredentials

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeUserScramCredentialsRequest struct {
	ApiVersion      int16
	Users           *[]DescribeUserScramCredentialsRequestUser // The users to describe, or null/empty to describe all users. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeUserScramCredentialsRequestUser struct {
	Name            *string // The user name. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeUserScramCredentialsRequest) Write(w io.Writer) error {
	// Users (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.usersEncoder, req.Users); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.usersEncoder, req.Users); err != nil {
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
func (req *DescribeUserScramCredentialsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeUserScramCredentialsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Users (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		users, err := protocol.ReadNullableCompactArray(r, req.usersDecoder)
		if err != nil {
			return err
		}
		req.Users = users
	} else {
		users, err := protocol.ReadNullableArray(r, req.usersDecoder)
		if err != nil {
			return err
		}
		req.Users = users
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

func (req *DescribeUserScramCredentialsRequest) usersEncoder(w io.Writer, value DescribeUserScramCredentialsRequestUser) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("DescribeUserScramCredentialsRequestUser.Name must not be nil in version %d", req.ApiVersion)
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

func (req *DescribeUserScramCredentialsRequest) usersDecoder(r io.Reader) (DescribeUserScramCredentialsRequestUser, error) {
	describeuserscramcredentialsrequestuser := DescribeUserScramCredentialsRequestUser{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeuserscramcredentialsrequestuser, err
		}
		describeuserscramcredentialsrequestuser.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return describeuserscramcredentialsrequestuser, err
		}
		describeuserscramcredentialsrequestuser.Name = &name
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeuserscramcredentialsrequestuser, err
		}
		describeuserscramcredentialsrequestuser.rawTaggedFields = &rawTaggedFields
	}

	return describeuserscramcredentialsrequestuser, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeUserScramCredentialsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeUserScramCredentialsRequest:\n")

	if req.Users != nil {
		fmt.Fprintf(w, "        Users:\n")
		for _, users := range *req.Users {
			fmt.Fprintf(w, "%s", users.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Users: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeUserScramCredentialsRequestUser) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	return w.String()
}
