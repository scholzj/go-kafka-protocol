package describecluster

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type DescribeClusterResponse struct {
	ApiVersion                  int16
	ThrottleTimeMs              int32                            // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ErrorCode                   int16                            // The top-level error code, or 0 if there was no error.
	ErrorMessage                *string                          // The top-level error message, or null if there was no error.
	EndpointType                int8                             // The endpoint type that was described. 1=brokers, 2=controllers.
	ClusterId                   *string                          // The cluster ID that responding broker belongs to.
	ControllerId                int32                            // The ID of the controller. When handled by a controller, returns the current voter leader ID. When handled by a broker, returns a random alive broker ID as a fallback.
	Brokers                     *[]DescribeClusterResponseBroker // Each broker in the response.
	ClusterAuthorizedOperations int32                            // 32-bit bitfield to represent authorized operations for this cluster.
	rawTaggedFields             *[]protocol.TaggedField
}

type DescribeClusterResponseBroker struct {
	BrokerId        int32   // The broker ID.
	Host            *string // The broker hostname.
	Port            int32   // The broker port.
	Rack            *string // The rack of the broker, or null if it has not been assigned to a rack.
	IsFenced        bool    // Whether the broker is fenced
	rawTaggedFields *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *DescribeClusterResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ErrorMessage (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, res.ErrorMessage); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, res.ErrorMessage); err != nil {
			return err
		}
	}

	// EndpointType (versions: 1+)
	if res.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, res.EndpointType); err != nil {
			return err
		}
	}

	// ClusterId (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *res.ClusterId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *res.ClusterId); err != nil {
			return err
		}
	}

	// ControllerId (versions: 0+)
	if err := protocol.WriteInt32(w, res.ControllerId); err != nil {
		return err
	}

	// Brokers (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, res.brokersEncoder, res.Brokers); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, res.brokersEncoder, *res.Brokers); err != nil {
			return err
		}
	}

	// ClusterAuthorizedOperations (versions: 0+)
	if err := protocol.WriteInt32(w, res.ClusterAuthorizedOperations); err != nil {
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
func (res *DescribeClusterResponse) Read(response protocol.Response) error {
	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

	var err error

	// ThrottleTimeMs (versions: 0+)
	throttletimems, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ThrottleTimeMs = throttletimems

	// ErrorCode (versions: 0+)
	errorcode, err := protocol.ReadInt16(r)
	if err != nil {
		return err
	}
	res.ErrorCode = errorcode

	// ErrorMessage (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		errormessage, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	} else {
		errormessage, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		res.ErrorMessage = errormessage
	}

	// EndpointType (versions: 1+)
	if response.ApiVersion >= 1 {
		endpointtype, err := protocol.ReadInt8(r)
		if err != nil {
			return err
		}
		res.EndpointType = endpointtype
	}

	// ClusterId (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		clusterid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		res.ClusterId = &clusterid
	} else {
		clusterid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		res.ClusterId = &clusterid
	}

	// ControllerId (versions: 0+)
	controllerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ControllerId = controllerid

	// Brokers (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		brokers, err := protocol.ReadNullableCompactArray(r, res.brokersDecoder)
		if err != nil {
			return err
		}
		res.Brokers = brokers
	} else {
		brokers, err := protocol.ReadArray(r, res.brokersDecoder)
		if err != nil {
			return err
		}
		res.Brokers = &brokers
	}

	// ClusterAuthorizedOperations (versions: 0+)
	clusterauthorizedoperations, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.ClusterAuthorizedOperations = clusterauthorizedoperations

	if isResponseFlexible(res.ApiVersion) {
		// Decode tagged fields
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return err
		}
		res.rawTaggedFields = &rawTaggedFields
		if err != nil {
			return err
		}
	}

	return nil
}

func (res *DescribeClusterResponse) brokersEncoder(w io.Writer, value DescribeClusterResponseBroker) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, value.BrokerId); err != nil {
		return err
	}

	// Host (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port (versions: 0+)
	if err := protocol.WriteInt32(w, value.Port); err != nil {
		return err
	}

	// Rack (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, value.Rack); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, value.Rack); err != nil {
			return err
		}
	}

	// IsFenced (versions: 2+)
	if res.ApiVersion >= 2 {
		if err := protocol.WriteBool(w, value.IsFenced); err != nil {
			return err
		}
	}

	// Tagged fields
	if isResponseFlexible(res.ApiVersion) {
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

func (res *DescribeClusterResponse) brokersDecoder(r io.Reader) (DescribeClusterResponseBroker, error) {
	describeclusterresponsebroker := DescribeClusterResponseBroker{}
	var err error

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return describeclusterresponsebroker, err
	}
	describeclusterresponsebroker.BrokerId = brokerid

	// Host (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadInt32(r)
	if err != nil {
		return describeclusterresponsebroker, err
	}
	describeclusterresponsebroker.Port = port

	// Rack (versions: 0+)
	if isRequestFlexible(res.ApiVersion) {
		rack, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.Rack = rack
	} else {
		rack, err := protocol.ReadNullableString(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.Rack = rack
	}

	// IsFenced (versions: 2+)
	if res.ApiVersion >= 2 {
		isfenced, err := protocol.ReadBool(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.IsFenced = isfenced
	}

	// Tagged fields
	if isRequestFlexible(res.ApiVersion) {
		var rawTaggedFields []protocol.TaggedField
		rawTaggedFields, err = protocol.ReadRawTaggedFields(r)
		if err != nil {
			return describeclusterresponsebroker, err
		}
		describeclusterresponsebroker.rawTaggedFields = &rawTaggedFields
	}

	return describeclusterresponsebroker, nil
}

//goland:noinspection GoUnhandledErrorResult
func (res *DescribeClusterResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- DescribeClusterResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	if res.ErrorMessage != nil {
		fmt.Fprintf(w, "        ErrorMessage: %v\n", *res.ErrorMessage)
	} else {
		fmt.Fprintf(w, "        ErrorMessage: nil\n")
	}
	fmt.Fprintf(w, "        EndpointType: %v\n", res.EndpointType)
	if res.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *res.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}
	fmt.Fprintf(w, "        ControllerId: %v\n", res.ControllerId)
	if res.Brokers != nil {
		fmt.Fprintf(w, "        Brokers:\n")
		for _, brokers := range *res.Brokers {
			fmt.Fprintf(w, "%s", brokers.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Brokers: nil\n")
	}
	fmt.Fprintf(w, "        ClusterAuthorizedOperations: %v\n", res.ClusterAuthorizedOperations)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *DescribeClusterResponseBroker) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            BrokerId: %v\n", value.BrokerId)
	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}
	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	if value.Rack != nil {
		fmt.Fprintf(w, "            Rack: %v\n", *value.Rack)
	} else {
		fmt.Fprintf(w, "            Rack: nil\n")
	}
	fmt.Fprintf(w, "            IsFenced: %v\n", value.IsFenced)

	return w.String()
}
