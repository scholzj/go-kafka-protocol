package expiredelegationtoken

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ExpireDelegationTokenRequest struct {
	ApiVersion         int16
	Hmac               *[]byte // The HMAC of the delegation token to be expired. (versions: 0+)
	ExpiryTimePeriodMs int64   // The expiry time period in milliseconds. (versions: 0+)
	rawTaggedFields    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *ExpireDelegationTokenRequest) Write(w io.Writer) error {
	// Hmac (versions: 0+)
	if req.Hmac == nil {
		return fmt.Errorf("ExpireDelegationTokenRequest.Hmac must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *req.Hmac); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *req.Hmac); err != nil {
			return err
		}
	}

	// ExpiryTimePeriodMs (versions: 0+)
	if err := protocol.WriteInt64(w, req.ExpiryTimePeriodMs); err != nil {
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
func (req *ExpireDelegationTokenRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ExpireDelegationTokenRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Hmac (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		hmac, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		req.Hmac = &hmac
	} else {
		hmac, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		req.Hmac = &hmac
	}

	// ExpiryTimePeriodMs (versions: 0+)
	expirytimeperiodms, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.ExpiryTimePeriodMs = expirytimeperiodms

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
func (req *ExpireDelegationTokenRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ExpireDelegationTokenRequest:\n")

	if req.Hmac != nil {
		fmt.Fprintf(w, "        Hmac: <%d bytes>\n", len(*req.Hmac))
	} else {
		fmt.Fprintf(w, "        Hmac: nil\n")
	}

	fmt.Fprintf(w, "        ExpiryTimePeriodMs: %v\n", req.ExpiryTimePeriodMs)

	return w.String()
}
