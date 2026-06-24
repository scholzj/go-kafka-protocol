package brokerregistration

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type BrokerRegistrationRequest struct {
	ApiVersion          int16
	BrokerId            int32                                // The broker ID. (versions: 0+)
	ClusterId           *string                              // The cluster id of the broker process. (versions: 0+)
	IncarnationId       uuid.UUID                            // The incarnation id of the broker process. (versions: 0+)
	Listeners           *[]BrokerRegistrationRequestListener // The listeners of this broker. (versions: 0+)
	Features            *[]BrokerRegistrationRequestFeature  // The features on this broker. Note: in v0-v3, features with MinSupportedVersion = 0 are omitted. (versions: 0+)
	Rack                *string                              // The rack which this broker is in. (versions: 0+, nullable: 0+)
	IsMigratingZkBroker bool                                 // If the required configurations for ZK migration are present, this value is set to true. (versions: 1+)
	LogDirs             *[]uuid.UUID                         // Log directories configured in this broker which are available. (versions: 2+)
	PreviousBrokerEpoch int64                                // The epoch before a clean shutdown. (versions: 3+)
	rawTaggedFields     *[]protocol.TaggedField
}

type BrokerRegistrationRequestListener struct {
	Name             *string // The name of the endpoint. (versions: 0+)
	Host             *string // The hostname. (versions: 0+)
	Port             uint16  // The port. (versions: 0+)
	SecurityProtocol int16   // The security protocol. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type BrokerRegistrationRequestFeature struct {
	Name                *string // The feature name. (versions: 0+)
	MinSupportedVersion int16   // The minimum supported feature level. (versions: 0+)
	MaxSupportedVersion int16   // The maximum supported feature level. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *BrokerRegistrationRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
		return err
	}

	// ClusterId (versions: 0+)
	if req.ClusterId == nil {
		return fmt.Errorf("BrokerRegistrationRequest.ClusterId must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *req.ClusterId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *req.ClusterId); err != nil {
			return err
		}
	}

	// IncarnationId (versions: 0+)
	if err := protocol.WriteUUID(w, req.IncarnationId); err != nil {
		return err
	}

	// Listeners (versions: 0+)
	if req.Listeners == nil {
		return fmt.Errorf("BrokerRegistrationRequest.Listeners must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.listenersEncoder, req.Listeners); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.listenersEncoder, *req.Listeners); err != nil {
			return err
		}
	}

	// Features (versions: 0+)
	if req.Features == nil {
		return fmt.Errorf("BrokerRegistrationRequest.Features must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.featuresEncoder, req.Features); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.featuresEncoder, *req.Features); err != nil {
			return err
		}
	}

	// Rack (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.Rack); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.Rack); err != nil {
			return err
		}
	}

	// IsMigratingZkBroker (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.IsMigratingZkBroker); err != nil {
			return err
		}
	}

	// LogDirs (versions: 2+)
	if req.ApiVersion >= 2 {
		if req.LogDirs == nil {
			return fmt.Errorf("BrokerRegistrationRequest.LogDirs must not be nil in version %d", req.ApiVersion)
		}
		if isRequestFlexible(req.ApiVersion) {
			if err := protocol.WriteNullableCompactArray(w, protocol.WriteUUID, req.LogDirs); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, protocol.WriteUUID, *req.LogDirs); err != nil {
				return err
			}
		}
	}

	// PreviousBrokerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		if err := protocol.WriteInt64(w, req.PreviousBrokerEpoch); err != nil {
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
func (req *BrokerRegistrationRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("BrokerRegistrationRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		clusterid, err := protocol.ReadCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = &clusterid
	} else {
		clusterid, err := protocol.ReadString(r)
		if err != nil {
			return err
		}
		req.ClusterId = &clusterid
	}

	// IncarnationId (versions: 0+)
	incarnationid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	req.IncarnationId = incarnationid

	// Listeners (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		listeners, err := protocol.ReadNullableCompactArray(r, req.listenersDecoder)
		if err != nil {
			return err
		}
		req.Listeners = listeners
	} else {
		listeners, err := protocol.ReadArray(r, req.listenersDecoder)
		if err != nil {
			return err
		}
		req.Listeners = &listeners
	}

	// Features (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		features, err := protocol.ReadNullableCompactArray(r, req.featuresDecoder)
		if err != nil {
			return err
		}
		req.Features = features
	} else {
		features, err := protocol.ReadArray(r, req.featuresDecoder)
		if err != nil {
			return err
		}
		req.Features = &features
	}

	// Rack (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		rack, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.Rack = rack
	} else {
		rack, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.Rack = rack
	}

	// IsMigratingZkBroker (versions: 1+)
	if req.ApiVersion >= 1 {
		ismigratingzkbroker, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.IsMigratingZkBroker = ismigratingzkbroker
	}

	// LogDirs (versions: 2+)
	if req.ApiVersion >= 2 {
		if isRequestFlexible(req.ApiVersion) {
			logdirs, err := protocol.ReadNullableCompactArray(r, protocol.ReadUUID)
			if err != nil {
				return err
			}
			req.LogDirs = logdirs
		} else {
			logdirs, err := protocol.ReadArray(r, protocol.ReadUUID)
			if err != nil {
				return err
			}
			req.LogDirs = &logdirs
		}
	}

	// PreviousBrokerEpoch (versions: 3+)
	if req.ApiVersion >= 3 {
		previousbrokerepoch, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		req.PreviousBrokerEpoch = previousbrokerepoch
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

func (req *BrokerRegistrationRequest) listenersEncoder(w io.Writer, value BrokerRegistrationRequestListener) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("BrokerRegistrationRequestListener.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// Host (versions: 0+)
	if value.Host == nil {
		return fmt.Errorf("BrokerRegistrationRequestListener.Host must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Host); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Host); err != nil {
			return err
		}
	}

	// Port (versions: 0+)
	if err := protocol.WriteUint16(w, value.Port); err != nil {
		return err
	}

	// SecurityProtocol (versions: 0+)
	if err := protocol.WriteInt16(w, value.SecurityProtocol); err != nil {
		return err
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *BrokerRegistrationRequest) listenersDecoder(r io.Reader) (BrokerRegistrationRequestListener, error) {
	brokerregistrationrequestlistener := BrokerRegistrationRequestListener{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return brokerregistrationrequestlistener, err
		}
		brokerregistrationrequestlistener.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return brokerregistrationrequestlistener, err
		}
		brokerregistrationrequestlistener.Name = &name
	}

	// Host (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return brokerregistrationrequestlistener, err
		}
		brokerregistrationrequestlistener.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return brokerregistrationrequestlistener, err
		}
		brokerregistrationrequestlistener.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadUInt16(r)
	if err != nil {
		return brokerregistrationrequestlistener, err
	}
	brokerregistrationrequestlistener.Port = port

	// SecurityProtocol (versions: 0+)
	securityprotocol, err := protocol.ReadInt16(r)
	if err != nil {
		return brokerregistrationrequestlistener, err
	}
	brokerregistrationrequestlistener.SecurityProtocol = securityprotocol

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return brokerregistrationrequestlistener, err
		}
		brokerregistrationrequestlistener.rawTaggedFields = &rawTaggedFields
	}

	return brokerregistrationrequestlistener, nil
}

func (req *BrokerRegistrationRequest) featuresEncoder(w io.Writer, value BrokerRegistrationRequestFeature) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("BrokerRegistrationRequestFeature.Name must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Name); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Name); err != nil {
			return err
		}
	}

	// MinSupportedVersion (versions: 0+)
	if err := protocol.WriteInt16(w, value.MinSupportedVersion); err != nil {
		return err
	}

	// MaxSupportedVersion (versions: 0+)
	if err := protocol.WriteInt16(w, value.MaxSupportedVersion); err != nil {
		return err
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
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

func (req *BrokerRegistrationRequest) featuresDecoder(r io.Reader) (BrokerRegistrationRequestFeature, error) {
	brokerregistrationrequestfeature := BrokerRegistrationRequestFeature{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return brokerregistrationrequestfeature, err
		}
		brokerregistrationrequestfeature.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return brokerregistrationrequestfeature, err
		}
		brokerregistrationrequestfeature.Name = &name
	}

	// MinSupportedVersion (versions: 0+)
	minsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return brokerregistrationrequestfeature, err
	}
	brokerregistrationrequestfeature.MinSupportedVersion = minsupportedversion

	// MaxSupportedVersion (versions: 0+)
	maxsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return brokerregistrationrequestfeature, err
	}
	brokerregistrationrequestfeature.MaxSupportedVersion = maxsupportedversion

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return brokerregistrationrequestfeature, err
		}
		brokerregistrationrequestfeature.rawTaggedFields = &rawTaggedFields
	}

	return brokerregistrationrequestfeature, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *BrokerRegistrationRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> BrokerRegistrationRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        IncarnationId: %v\n", req.IncarnationId)

	if req.Listeners != nil {
		fmt.Fprintf(w, "        Listeners:\n")
		for _, listeners := range *req.Listeners {
			fmt.Fprintf(w, "%s", listeners.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Listeners: nil\n")
	}

	if req.Features != nil {
		fmt.Fprintf(w, "        Features:\n")
		for _, features := range *req.Features {
			fmt.Fprintf(w, "%s", features.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Features: nil\n")
	}

	if req.Rack != nil {
		fmt.Fprintf(w, "        Rack: %v\n", *req.Rack)
	} else {
		fmt.Fprintf(w, "        Rack: nil\n")
	}

	fmt.Fprintf(w, "        IsMigratingZkBroker: %v\n", req.IsMigratingZkBroker)

	if req.LogDirs != nil {
		fmt.Fprintf(w, "        LogDirs: %v\n", *req.LogDirs)
	} else {
		fmt.Fprintf(w, "        LogDirs: nil\n")
	}

	fmt.Fprintf(w, "        PreviousBrokerEpoch: %v\n", req.PreviousBrokerEpoch)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *BrokerRegistrationRequestListener) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	if value.Host != nil {
		fmt.Fprintf(w, "            Host: %v\n", *value.Host)
	} else {
		fmt.Fprintf(w, "            Host: nil\n")
	}

	fmt.Fprintf(w, "            Port: %v\n", value.Port)
	fmt.Fprintf(w, "            SecurityProtocol: %v\n", value.SecurityProtocol)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *BrokerRegistrationRequestFeature) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "            Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "            Name: nil\n")
	}

	fmt.Fprintf(w, "            MinSupportedVersion: %v\n", value.MinSupportedVersion)
	fmt.Fprintf(w, "            MaxSupportedVersion: %v\n", value.MaxSupportedVersion)

	return w.String()
}
