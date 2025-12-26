package apiversions

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type ApiVersionsRequest struct {
	ApiVersion            int16
	ClientSoftwareName    *string
	ClientSoftwareVersion *string
	rawTaggedFields       []protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (req *ApiVersionsRequest) Write(w io.Writer) error {
	// ClientSoftwareName
	if req.ApiVersion >= 3 {
		if err := protocol.WriteCompactString(w, *req.ClientSoftwareName); err != nil {
			return err
		}
	}

	// ClientSoftwareVersion
	if req.ApiVersion >= 3 {
		if err := protocol.WriteCompactString(w, *req.ClientSoftwareVersion); err != nil {
			return err
		}
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteRawTaggedFields(w, req.rawTaggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *ApiVersionsRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClientSoftwareName
	if request.ApiVersion >= 3 {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.ClientSoftwareName = &name
	}

	// ClientSoftwareVersion
	if request.ApiVersion >= 3 {
		version, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.ClientSoftwareVersion = &version
	}

	// Tagged fields
	if isRequestFlexible(request.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ApiVersionsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "-> ApiVersionsRequest:\n")
	fmt.Fprintf(w, "        ClientSoftwareName: %s\n", *req.ClientSoftwareName)
	fmt.Fprintf(w, "        ClientSoftwareVersion: %s\n", *req.ClientSoftwareVersion)

	return w.String()
}
