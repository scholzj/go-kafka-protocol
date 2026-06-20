package createdelegationtoken

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type CreateDelegationTokenResponse struct {
	ApiVersion                  int16
	ErrorCode                   int16   // The top-level error, or zero if there was no error. (versions: 0+)
	PrincipalType               *string // The principal type of the token owner. (versions: 0+)
	PrincipalName               *string // The name of the token owner. (versions: 0+)
	TokenRequesterPrincipalType *string // The principal type of the requester of the token. (versions: 3+)
	TokenRequesterPrincipalName *string // The principal type of the requester of the token. (versions: 3+)
	IssueTimestampMs            int64   // When this token was generated. (versions: 0+)
	ExpiryTimestampMs           int64   // When this token expires. (versions: 0+)
	MaxTimestampMs              int64   // The maximum lifetime of this token. (versions: 0+)
	TokenId                     *string // The token UUID. (versions: 0+)
	Hmac                        *[]byte // HMAC of the delegation token. (versions: 0+)
	ThrottleTimeMs              int32   // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	rawTaggedFields             *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (res *CreateDelegationTokenResponse) Write(w io.Writer) error {
	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// PrincipalType (versions: 0+)
	if res.PrincipalType == nil {
		return fmt.Errorf("CreateDelegationTokenResponse.PrincipalType must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.PrincipalType); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.PrincipalType); err != nil {
			return err
		}
	}

	// PrincipalName (versions: 0+)
	if res.PrincipalName == nil {
		return fmt.Errorf("CreateDelegationTokenResponse.PrincipalName must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.PrincipalName); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.PrincipalName); err != nil {
			return err
		}
	}

	// TokenRequesterPrincipalType (versions: 3+)
	if res.ApiVersion >= 3 {
		if res.TokenRequesterPrincipalType == nil {
			return fmt.Errorf("CreateDelegationTokenResponse.TokenRequesterPrincipalType must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *res.TokenRequesterPrincipalType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.TokenRequesterPrincipalType); err != nil {
				return err
			}
		}
	}

	// TokenRequesterPrincipalName (versions: 3+)
	if res.ApiVersion >= 3 {
		if res.TokenRequesterPrincipalName == nil {
			return fmt.Errorf("CreateDelegationTokenResponse.TokenRequesterPrincipalName must not be nil in version %d", res.ApiVersion)
		}
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteCompactString(w, *res.TokenRequesterPrincipalName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *res.TokenRequesterPrincipalName); err != nil {
				return err
			}
		}
	}

	// IssueTimestampMs (versions: 0+)
	if err := protocol.WriteInt64(w, res.IssueTimestampMs); err != nil {
		return err
	}

	// ExpiryTimestampMs (versions: 0+)
	if err := protocol.WriteInt64(w, res.ExpiryTimestampMs); err != nil {
		return err
	}

	// MaxTimestampMs (versions: 0+)
	if err := protocol.WriteInt64(w, res.MaxTimestampMs); err != nil {
		return err
	}

	// TokenId (versions: 0+)
	if res.TokenId == nil {
		return fmt.Errorf("CreateDelegationTokenResponse.TokenId must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.TokenId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.TokenId); err != nil {
			return err
		}
	}

	// Hmac (versions: 0+)
	if res.Hmac == nil {
		return fmt.Errorf("CreateDelegationTokenResponse.Hmac must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *res.Hmac); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *res.Hmac); err != nil {
			return err
		}
	}

	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
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
func (res *CreateDelegationTokenResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("CreateDelegationTokenResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// PrincipalType (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principaltype, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.PrincipalType = &principaltype
	} else {
		principaltype, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.PrincipalType = &principaltype
	}

	// PrincipalName (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		principalname, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.PrincipalName = &principalname
	} else {
		principalname, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.PrincipalName = &principalname
	}

	// TokenRequesterPrincipalType (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			tokenrequesterprincipaltype, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			res.TokenRequesterPrincipalType = &tokenrequesterprincipaltype
		} else {
			tokenrequesterprincipaltype, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.TokenRequesterPrincipalType = &tokenrequesterprincipaltype
		}
	}

	// TokenRequesterPrincipalName (versions: 3+)
	if res.ApiVersion >= 3 {
		if isResponseFlexible(res.ApiVersion) {
			tokenrequesterprincipalname, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			res.TokenRequesterPrincipalName = &tokenrequesterprincipalname
		} else {
			tokenrequesterprincipalname, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			res.TokenRequesterPrincipalName = &tokenrequesterprincipalname
		}
	}

	// IssueTimestampMs (versions: 0+)
	issuetimestampms, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	res.IssueTimestampMs = issuetimestampms

	// ExpiryTimestampMs (versions: 0+)
	expirytimestampms, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	res.ExpiryTimestampMs = expirytimestampms

	// MaxTimestampMs (versions: 0+)
	maxtimestampms, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	res.MaxTimestampMs = maxtimestampms

	// TokenId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		tokenid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.TokenId = &tokenid
	} else {
		tokenid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.TokenId = &tokenid
	}

	// Hmac (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		hmac, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		res.Hmac = &hmac
	} else {
		hmac, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		res.Hmac = &hmac
	}

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

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

//goland:noinspection GoUnhandledErrorResult
func (res *CreateDelegationTokenResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- CreateDelegationTokenResponse:\n")
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.PrincipalType != nil {
		fmt.Fprintf(w, "        PrincipalType: %v\n", *res.PrincipalType)
	} else {
		fmt.Fprintf(w, "        PrincipalType: nil\n")
	}

	if res.PrincipalName != nil {
		fmt.Fprintf(w, "        PrincipalName: %v\n", *res.PrincipalName)
	} else {
		fmt.Fprintf(w, "        PrincipalName: nil\n")
	}

	if res.TokenRequesterPrincipalType != nil {
		fmt.Fprintf(w, "        TokenRequesterPrincipalType: %v\n", *res.TokenRequesterPrincipalType)
	} else {
		fmt.Fprintf(w, "        TokenRequesterPrincipalType: nil\n")
	}

	if res.TokenRequesterPrincipalName != nil {
		fmt.Fprintf(w, "        TokenRequesterPrincipalName: %v\n", *res.TokenRequesterPrincipalName)
	} else {
		fmt.Fprintf(w, "        TokenRequesterPrincipalName: nil\n")
	}

	fmt.Fprintf(w, "        IssueTimestampMs: %v\n", res.IssueTimestampMs)
	fmt.Fprintf(w, "        ExpiryTimestampMs: %v\n", res.ExpiryTimestampMs)
	fmt.Fprintf(w, "        MaxTimestampMs: %v\n", res.MaxTimestampMs)

	if res.TokenId != nil {
		fmt.Fprintf(w, "        TokenId: %v\n", *res.TokenId)
	} else {
		fmt.Fprintf(w, "        TokenId: nil\n")
	}

	if res.Hmac != nil {
		fmt.Fprintf(w, "        Hmac: <%d bytes>\n", len(*res.Hmac))
	} else {
		fmt.Fprintf(w, "        Hmac: nil\n")
	}

	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)

	return w.String()
}
