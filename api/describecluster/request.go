package describecluster

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeClusterRequest struct {
	ApiVersion                         int16
	IncludeClusterAuthorizedOperations bool // Whether to include cluster authorized operations.
	EndpointType                       int8 // The endpoint type to describe. 1=brokers, 2=controllers.
	IncludeFencedBrokers               bool // Whether to include fenced brokers when listing brokers.
	rawTaggedFields                    *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *DescribeClusterRequest) Write(w io.Writer) error {
	// IncludeClusterAuthorizedOperations (versions: 0+)
	if err := protocol.WriteBool(w, req.IncludeClusterAuthorizedOperations); err != nil {
		return err
	}

	// EndpointType (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, req.EndpointType); err != nil {
			return err
		}
	}

	// IncludeFencedBrokers (versions: 2+)
	if req.ApiVersion >= 2 {
		if err := protocol.WriteBool(w, req.IncludeFencedBrokers); err != nil {
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
func (req *DescribeClusterRequest) Read(request protocol.Request) error {
	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	var err error

	// IncludeClusterAuthorizedOperations (versions: 0+)
	includeclusterauthorizedoperations, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.IncludeClusterAuthorizedOperations = includeclusterauthorizedoperations

	// EndpointType (versions: 1+)
	if request.ApiVersion >= 1 {
		endpointtype, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		req.EndpointType = endpointtype
	}

	// IncludeFencedBrokers (versions: 2+)
	if request.ApiVersion >= 2 {
		includefencedbrokers, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IncludeFencedBrokers = includefencedbrokers
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		req.rawTaggedFields = &rawTaggedFields
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *DescribeClusterRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> DescribeClusterRequest:\n")
	fmt.Fprintf(w, "        IncludeClusterAuthorizedOperations: %v\n", req.IncludeClusterAuthorizedOperations)
	fmt.Fprintf(w, "        EndpointType: %v\n", req.EndpointType)
	fmt.Fprintf(w, "        IncludeFencedBrokers: %v\n", req.IncludeFencedBrokers)

	return w.String()
}
