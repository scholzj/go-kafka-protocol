package envelope

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type EnvelopeRequest struct {
	ApiVersion        int16
	RequestData       *[]byte // The embedded request header and data. (versions: 0+)
	RequestPrincipal  *[]byte // Value of the initial client principal when the request is redirected by a broker. (versions: 0+, nullable: 0+)
	ClientHostAddress *[]byte // The original client's address in bytes. (versions: 0+)
	rawTaggedFields   *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *EnvelopeRequest) Write(w io.Writer) error {
	// RequestData (versions: 0+)
	if req.RequestData == nil {
		return fmt.Errorf("EnvelopeRequest.RequestData must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *req.RequestData); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *req.RequestData); err != nil {
			return err
		}
	}

	// RequestPrincipal (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactBytes(w, req.RequestPrincipal); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableBytes(w, req.RequestPrincipal); err != nil {
			return err
		}
	}

	// ClientHostAddress (versions: 0+)
	if req.ClientHostAddress == nil {
		return fmt.Errorf("EnvelopeRequest.ClientHostAddress must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *req.ClientHostAddress); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *req.ClientHostAddress); err != nil {
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
func (req *EnvelopeRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("EnvelopeRequest.Read: request or its body is nil")
	}

	*req = EnvelopeRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// RequestData (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		requestdata, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		req.RequestData = &requestdata
	} else {
		requestdata, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		req.RequestData = &requestdata
	}

	// RequestPrincipal (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		requestprincipal, err := protocol.ReadNullableCompactBytes(r)
		if err != nil {
			return err
		}
		req.RequestPrincipal = requestprincipal
	} else {
		requestprincipal, err := protocol.ReadNullableBytes(r)
		if err != nil {
			return err
		}
		req.RequestPrincipal = requestprincipal
	}

	// ClientHostAddress (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		clienthostaddress, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		req.ClientHostAddress = &clienthostaddress
	} else {
		clienthostaddress, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		req.ClientHostAddress = &clienthostaddress
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
func (req *EnvelopeRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> EnvelopeRequest:\n")

	if req.RequestData != nil {
		fmt.Fprintf(w, "        RequestData: <%d bytes>\n", len(*req.RequestData))
	} else {
		fmt.Fprintf(w, "        RequestData: nil\n")
	}

	if req.RequestPrincipal != nil {
		fmt.Fprintf(w, "        RequestPrincipal: <%d bytes>\n", len(*req.RequestPrincipal))
	} else {
		fmt.Fprintf(w, "        RequestPrincipal: nil\n")
	}

	if req.ClientHostAddress != nil {
		fmt.Fprintf(w, "        ClientHostAddress: <%d bytes>\n", len(*req.ClientHostAddress))
	} else {
		fmt.Fprintf(w, "        ClientHostAddress: nil\n")
	}

	return w.String()
}
