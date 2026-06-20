package apiversions

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ApiVersionsRequest struct {
	ApiVersion            int16
	ClientSoftwareName    *string // The name of the client. (versions: 3+)
	ClientSoftwareVersion *string // The version of the client. (versions: 3+)
	rawTaggedFields       *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *ApiVersionsRequest) Write(w io.Writer) error {
	// ClientSoftwareName (versions: 3+)
	if req.ApiVersion >= 3 {
		if req.ClientSoftwareName == nil {
			return fmt.Errorf("ApiVersionsRequest.ClientSoftwareName must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.ClientSoftwareName); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.ClientSoftwareName); err != nil {
				return err
			}
		}
	}

	// ClientSoftwareVersion (versions: 3+)
	if req.ApiVersion >= 3 {
		if req.ClientSoftwareVersion == nil {
			return fmt.Errorf("ApiVersionsRequest.ClientSoftwareVersion must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteCompactString(w, *req.ClientSoftwareVersion); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteString(w, *req.ClientSoftwareVersion); err != nil {
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
func (req *ApiVersionsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ApiVersionsRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClientSoftwareName (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			clientsoftwarename, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.ClientSoftwareName = &clientsoftwarename
		} else {
			clientsoftwarename, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.ClientSoftwareName = &clientsoftwarename
		}
	}

	// ClientSoftwareVersion (versions: 3+)
	if req.ApiVersion >= 3 {
		if isRequestFlexible(req.ApiVersion) {
			clientsoftwareversion, err := protocol.ReadCompactString(r)
			if err != nil {
				return err
			}
			req.ClientSoftwareVersion = &clientsoftwareversion
		} else {
			clientsoftwareversion, err := protocol.ReadString(r)
			if err != nil {
				return err
			}
			req.ClientSoftwareVersion = &clientsoftwareversion
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
func (req *ApiVersionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ApiVersionsRequest:\n")

	if req.ClientSoftwareName != nil {
		fmt.Fprintf(w, "        ClientSoftwareName: %v\n", *req.ClientSoftwareName)
	} else {
		fmt.Fprintf(w, "        ClientSoftwareName: nil\n")
	}

	if req.ClientSoftwareVersion != nil {
		fmt.Fprintf(w, "        ClientSoftwareVersion: %v\n", *req.ClientSoftwareVersion)
	} else {
		fmt.Fprintf(w, "        ClientSoftwareVersion: nil\n")
	}

	return w.String()
}
