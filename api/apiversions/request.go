package apiversions

import (
	"bytes"
	"fmt"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

type ApiVersionsRequest struct {
	ClientSoftwareName    *string
	ClientSoftwareVersion *string
	rawTaggedFields       []protocol.TaggedField
}

func RequestHeaderVersion(apiVersion int16) int16 {
	if isRequestFlexible(apiVersion) {
		return 2
	} else {
		return 1
	}
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 3
}

func (r *ApiVersionsRequest) Write(w io.Writer, apiVersion int16) error {
	if apiVersion >= 3 {
		// ClientSoftwareName
		err := protocol.WriteCompactString(w, *r.ClientSoftwareName)
		if err != nil {
			return err
		}

		// ClientSoftwareVersion
		err = protocol.WriteCompactString(w, *r.ClientSoftwareVersion)
		if err != nil {
			return err
		}

		// Tagged fields
		err = protocol.WriteRawTaggedFields(w, r.rawTaggedFields)
		if err != nil {
			return err
		}

		return nil
	} else {
		return nil
	}
}

// TODO: pass version and bytes only
func (r *ApiVersionsRequest) Read(request protocol.Request) error {
	reader := bytes.NewBuffer(request.Body.Bytes())

	if request.ApiVersion >= 3 {
		// ClientSoftwareName
		name, err := protocol.ReadCompactString(reader)
		if err != nil {
			return err
		}

		fmt.Printf("ClientSoftwareName: %s\n", name)
		r.ClientSoftwareName = &name

		// ClientSoftwareVersion
		version, err := protocol.ReadCompactString(reader)
		if err != nil {
			return err
		}

		fmt.Printf("ClientSoftwareVersion: %s\n", version)
		r.ClientSoftwareVersion = &version

		// Tagged fields
		rawTaggedFields, err := protocol.ReadRawTaggedFields(reader)
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return err
		}
		r.rawTaggedFields = rawTaggedFields

		return nil
	} else {
		return nil
	}
}

func (r *ApiVersionsRequest) Decode(request protocol.Request) error {
	bytes := request.Body.Bytes()
	offset := 0

	if request.ApiVersion >= 3 {
		// ClientSoftwareName
		name, c, err := protocol.DecodeCompactString(bytes[offset:])
		if err != nil {
			return err
		}
		offset += c

		fmt.Printf("ClientSoftwareName: %s\n", name)
		r.ClientSoftwareName = &name

		// ClientSoftwareVersion
		version, c, err := protocol.DecodeCompactString(bytes[offset:])
		if err != nil {
			return err
		}
		offset += c

		fmt.Printf("ClientSoftwareVersion: %s\n", version)
		r.ClientSoftwareVersion = &version

		// Tagged fields
		rawTaggedFields, c, err := protocol.DecodeRawTaggedFields(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return err
		}
		offset += c
		r.rawTaggedFields = rawTaggedFields

		return nil
	} else {
		return nil
	}
}

func (r *ApiVersionsRequest) PrettyPrint() {
	fmt.Printf("-> ApiVersionsRequest:\n")
	fmt.Printf("        ClientSoftwareName: %s\n", *r.ClientSoftwareName)
	fmt.Printf("        ClientSoftwareVersion: %s\n", *r.ClientSoftwareVersion)
	fmt.Printf("\n")
}
