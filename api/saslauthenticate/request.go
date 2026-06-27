package saslauthenticate

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type SaslAuthenticateRequest struct {
	ApiVersion      int16
	AuthBytes       *[]byte // The SASL authentication bytes from the client, as defined by the SASL mechanism. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *SaslAuthenticateRequest) Write(w io.Writer) error {
	// AuthBytes (versions: 0+)
	if req.AuthBytes == nil {
		return fmt.Errorf("SaslAuthenticateRequest.AuthBytes must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactBytes(w, *req.AuthBytes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteBytes(w, *req.AuthBytes); err != nil {
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
func (req *SaslAuthenticateRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("SaslAuthenticateRequest.Read: request or its body is nil")
	}

	*req = SaslAuthenticateRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// AuthBytes (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		authbytes, err := protocol.ReadCompactBytes(r)
		if err != nil {
			return err
		}
		req.AuthBytes = &authbytes
	} else {
		authbytes, err := protocol.ReadBytes(r)
		if err != nil {
			return err
		}
		req.AuthBytes = &authbytes
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
func (req *SaslAuthenticateRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> SaslAuthenticateRequest:\n")

	if req.AuthBytes != nil {
		fmt.Fprintf(w, "        AuthBytes: <%d bytes>\n", len(*req.AuthBytes))
	} else {
		fmt.Fprintf(w, "        AuthBytes: nil\n")
	}

	return w.String()
}
