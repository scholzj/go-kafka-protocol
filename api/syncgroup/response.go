package syncgroup

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SyncGroupResponse struct {
	ApiVersion      int16
	ThrottleTimeMs  int32   // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 1+)
	ErrorCode       int16   // The error code, or 0 if there was no error. (versions: 0+)
	ProtocolType    *string // The group protocol type. (versions: 5+, nullable: 5+)
	ProtocolName    *string // The group protocol name. (versions: 5+, nullable: 5+)
	Assignment      *[]byte // The member assignment. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 4
}

func (res *SyncGroupResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
			return err
		}
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ProtocolType (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.ProtocolType); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ProtocolType); err != nil {
				return err
			}
		}
	}

	// ProtocolName (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			if err := protocol.WriteNullableCompactString(w, res.ProtocolName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, res.ProtocolName); err != nil {
				return err
			}
		}
	}

	// Assignment (versions: 0+)
	if res.Assignment == nil {
		return fmt.Errorf("SyncGroupResponse.Assignment must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *res.Assignment); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *res.Assignment); err != nil {
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
func (res *SyncGroupResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("SyncGroupResponse.Read: response or its body is nil")
	}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	// ThrottleTimeMs (versions: 1+)
	if res.ApiVersion >= 1 {
		throttletimems, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		res.ThrottleTimeMs = throttletimems
	}

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ProtocolType (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			protocoltype, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			res.ProtocolType = protocoltype
		} else {
			protocoltype, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			res.ProtocolType = protocoltype
		}
	}

	// ProtocolName (versions: 5+)
	if res.ApiVersion >= 5 {
		if isResponseFlexible(res.ApiVersion) {
			protocolname, err := protocol.ReadNullableCompactString(r)
			if err != nil {
				return err
			}
			res.ProtocolName = protocolname
		} else {
			protocolname, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			res.ProtocolName = protocolname
		}
	}

	// Assignment (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		assignment, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		res.Assignment = &assignment
	} else {
		assignment, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		res.Assignment = &assignment
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

//goland:noinspection GoUnhandledErrorResult
func (res *SyncGroupResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- SyncGroupResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)

	if res.ProtocolType != nil {
		fmt.Fprintf(w, "        ProtocolType: %v\n", *res.ProtocolType)
	} else {
		fmt.Fprintf(w, "        ProtocolType: nil\n")
	}

	if res.ProtocolName != nil {
		fmt.Fprintf(w, "        ProtocolName: %v\n", *res.ProtocolName)
	} else {
		fmt.Fprintf(w, "        ProtocolName: nil\n")
	}

	if res.Assignment != nil {
		fmt.Fprintf(w, "        Assignment: <%d bytes>\n", len(*res.Assignment))
	} else {
		fmt.Fprintf(w, "        Assignment: nil\n")
	}

	return w.String()
}
