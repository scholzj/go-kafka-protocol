package describedelegationtoken

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeDelegationTokenRequest struct {
	ApiVersion      int16
	Owners          *[]DescribeDelegationTokenRequestOwner // Each owner that we want to describe delegation tokens for, or null to describe all tokens. (versions: 0+, nullable: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type DescribeDelegationTokenRequestOwner struct {
	PrincipalType   *string // The owner principal type. (versions: 0+)
	PrincipalName   *string // The owner principal name. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *DescribeDelegationTokenRequest) Write(w io.Writer) error {
	// Owners (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.ownersEncoder, req.Owners); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableArray(w, req.ownersEncoder, req.Owners); err != nil {
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
func (req *DescribeDelegationTokenRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("DescribeDelegationTokenRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Owners (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		owners, err := protocol.ReadNullableCompactArray(r, req.ownersDecoder)
		if err != nil {
			return err
		}
		req.Owners = owners
	} else {
		owners, err := protocol.ReadNullableArray(r, req.ownersDecoder)
		if err != nil {
			return err
		}
		req.Owners = owners
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

func (req *DescribeDelegationTokenRequest) ownersEncoder(w io.Writer, value DescribeDelegationTokenRequestOwner) error {
	// PrincipalType (versions: 0+)
	if value.PrincipalType == nil {
		return fmt.Errorf("DescribeDelegationTokenRequestOwner.PrincipalType must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalType); err != nil {
			return err
		}
	}

	// PrincipalName (versions: 0+)
	if value.PrincipalName == nil {
		return fmt.Errorf("DescribeDelegationTokenRequestOwner.PrincipalName must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.PrincipalName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.PrincipalName); err != nil {
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

func (req *DescribeDelegationTokenRequest) ownersDecoder(r io.Reader) (DescribeDelegationTokenRequestOwner, error) {
	describedelegationtokenrequestowner := DescribeDelegationTokenRequestOwner{}

	// PrincipalType (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principaltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenrequestowner, err
		}
		describedelegationtokenrequestowner.PrincipalType = &principaltype
	} else {
		principaltype, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenrequestowner, err
		}
		describedelegationtokenrequestowner.PrincipalType = &principaltype
	}

	// PrincipalName (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		principalname, err := protocol.ReadCompactString(r)
		if err != nil {
			return describedelegationtokenrequestowner, err
		}
		describedelegationtokenrequestowner.PrincipalName = &principalname
	} else {
		principalname, err := protocol.ReadString(r)
		if err != nil {
			return describedelegationtokenrequestowner, err
		}
		describedelegationtokenrequestowner.PrincipalName = &principalname
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describedelegationtokenrequestowner, err
		}
		describedelegationtokenrequestowner.rawTaggedFields = &rawTaggedFields
	}

	return describedelegationtokenrequestowner, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeDelegationTokenRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeDelegationTokenRequest:\n")

	if req.Owners != nil {
		fmt.Fprintf(w, "        Owners:\n")
		for _, owners := range *req.Owners {
			fmt.Fprintf(w, "%s", owners.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Owners: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeDelegationTokenRequestOwner) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.PrincipalType != nil {
		fmt.Fprintf(w, "            PrincipalType: %v\n", *value.PrincipalType)
	} else {
		fmt.Fprintf(w, "            PrincipalType: nil\n")
	}

	if value.PrincipalName != nil {
		fmt.Fprintf(w, "            PrincipalName: %v\n", *value.PrincipalName)
	} else {
		fmt.Fprintf(w, "            PrincipalName: nil\n")
	}

	return w.String()
}
