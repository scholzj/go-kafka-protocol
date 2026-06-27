package gettelemetrysubscriptions

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type GetTelemetrySubscriptionsResponse struct {
	ApiVersion               int16
	ThrottleTimeMs           int32     // The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota. (versions: 0+)
	ErrorCode                int16     // The error code, or 0 if there was no error. (versions: 0+)
	ClientInstanceId         uuid.UUID // Assigned client instance id if ClientInstanceId was 0 in the request, else 0. (versions: 0+)
	SubscriptionId           int32     // Unique identifier for the current subscription set for this client instance. (versions: 0+)
	AcceptedCompressionTypes *[]int8   // Compression types that broker accepts for the PushTelemetryRequest. (versions: 0+)
	PushIntervalMs           int32     // Configured push interval, which is the lowest configured interval in the current subscription set. (versions: 0+)
	TelemetryMaxBytes        int32     // The maximum bytes of binary data the broker accepts in PushTelemetryRequest. (versions: 0+)
	DeltaTemporality         bool      // Flag to indicate monotonic/counter metrics are to be emitted as deltas or cumulative values. (versions: 0+)
	RequestedMetrics         *[]string // Requested metrics prefix string match. Empty array: No metrics subscribed, Array[0] empty string: All metrics subscribed. (versions: 0+)
	rawTaggedFields          *[]protocol.TaggedField
}

func isResponseFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (res *GetTelemetrySubscriptionsResponse) Write(w io.Writer) error {
	// ThrottleTimeMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.ThrottleTimeMs); err != nil {
		return err
	}

	// ErrorCode (versions: 0+)
	if err := protocol.WriteInt16(w, res.ErrorCode); err != nil {
		return err
	}

	// ClientInstanceId (versions: 0+)
	if err := protocol.WriteUUID(w, res.ClientInstanceId); err != nil {
		return err
	}

	// SubscriptionId (versions: 0+)
	if err := protocol.WriteInt32(w, res.SubscriptionId); err != nil {
		return err
	}

	// AcceptedCompressionTypes (versions: 0+)
	if res.AcceptedCompressionTypes == nil {
		return fmt.Errorf("GetTelemetrySubscriptionsResponse.AcceptedCompressionTypes must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt8, res.AcceptedCompressionTypes); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt8, *res.AcceptedCompressionTypes); err != nil {
			return err
		}
	}

	// PushIntervalMs (versions: 0+)
	if err := protocol.WriteInt32(w, res.PushIntervalMs); err != nil {
		return err
	}

	// TelemetryMaxBytes (versions: 0+)
	if err := protocol.WriteInt32(w, res.TelemetryMaxBytes); err != nil {
		return err
	}

	// DeltaTemporality (versions: 0+)
	if err := protocol.WriteBool(w, res.DeltaTemporality); err != nil {
		return err
	}

	// RequestedMetrics (versions: 0+)
	if res.RequestedMetrics == nil {
		return fmt.Errorf("GetTelemetrySubscriptionsResponse.RequestedMetrics must not be nil in version %d", res.ApiVersion)
	}
	if isResponseFlexible(res.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteCompactString, res.RequestedMetrics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteString, *res.RequestedMetrics); err != nil {
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
func (res *GetTelemetrySubscriptionsResponse) Read(response *protocol.Response) error {
	if response == nil || response.Body == nil {
		return fmt.Errorf("GetTelemetrySubscriptionsResponse.Read: response or its body is nil")
	}

	*res = GetTelemetrySubscriptionsResponse{}

	r := bytes.NewBuffer(response.Body.Bytes())
	res.ApiVersion = response.ApiVersion

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

	// ClientInstanceId (versions: 0+)
	clientinstanceid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	res.ClientInstanceId = clientinstanceid

	// SubscriptionId (versions: 0+)
	subscriptionid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.SubscriptionId = subscriptionid

	// AcceptedCompressionTypes (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		acceptedcompressiontypes, err := protocol.ReadCompactArray(r, protocol.ReadInt8)
		if err != nil {
			return err
		}
		res.AcceptedCompressionTypes = &acceptedcompressiontypes
	} else {
		acceptedcompressiontypes, err := protocol.ReadArray(r, protocol.ReadInt8)
		if err != nil {
			return err
		}
		res.AcceptedCompressionTypes = &acceptedcompressiontypes
	}

	// PushIntervalMs (versions: 0+)
	pushintervalms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.PushIntervalMs = pushintervalms

	// TelemetryMaxBytes (versions: 0+)
	telemetrymaxbytes, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	res.TelemetryMaxBytes = telemetrymaxbytes

	// DeltaTemporality (versions: 0+)
	deltatemporality, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	res.DeltaTemporality = deltatemporality

	// RequestedMetrics (versions: 0+)
	if isResponseFlexible(res.ApiVersion) {
		requestedmetrics, err := protocol.ReadCompactArray(r, protocol.ReadCompactString)
		if err != nil {
			return err
		}
		res.RequestedMetrics = &requestedmetrics
	} else {
		requestedmetrics, err := protocol.ReadArray(r, protocol.ReadString)
		if err != nil {
			return err
		}
		res.RequestedMetrics = &requestedmetrics
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
func (res *GetTelemetrySubscriptionsResponse) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    <- GetTelemetrySubscriptionsResponse:\n")
	fmt.Fprintf(w, "        ThrottleTimeMs: %v\n", res.ThrottleTimeMs)
	fmt.Fprintf(w, "        ErrorCode: %v\n", res.ErrorCode)
	fmt.Fprintf(w, "        ClientInstanceId: %v\n", res.ClientInstanceId)
	fmt.Fprintf(w, "        SubscriptionId: %v\n", res.SubscriptionId)

	if res.AcceptedCompressionTypes != nil {
		fmt.Fprintf(w, "        AcceptedCompressionTypes: %v\n", *res.AcceptedCompressionTypes)
	} else {
		fmt.Fprintf(w, "        AcceptedCompressionTypes: nil\n")
	}

	fmt.Fprintf(w, "        PushIntervalMs: %v\n", res.PushIntervalMs)
	fmt.Fprintf(w, "        TelemetryMaxBytes: %v\n", res.TelemetryMaxBytes)
	fmt.Fprintf(w, "        DeltaTemporality: %v\n", res.DeltaTemporality)

	if res.RequestedMetrics != nil {
		fmt.Fprintf(w, "        RequestedMetrics: %v\n", *res.RequestedMetrics)
	} else {
		fmt.Fprintf(w, "        RequestedMetrics: nil\n")
	}

	return w.String()
}
