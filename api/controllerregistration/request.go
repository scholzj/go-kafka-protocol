package controllerregistration

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type ControllerRegistrationRequest struct {
	ApiVersion       int16
	ControllerId     int32                                    // The ID of the controller to register. (versions: 0+)
	IncarnationId    uuid.UUID                                // The controller incarnation ID, which is unique to each process run. (versions: 0+)
	ZkMigrationReady bool                                     // Set if the required configurations for ZK migration are present. (versions: 0+)
	Listeners        *[]ControllerRegistrationRequestListener // The listeners of this controller. (versions: 0+)
	Features         *[]ControllerRegistrationRequestFeature  // The features on this controller. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type ControllerRegistrationRequestListener struct {
	Name             *string // The name of the endpoint. (versions: 0+)
	Host             *string // The hostname. (versions: 0+)
	Port             uint16  // The port. (versions: 0+)
	SecurityProtocol int16   // The security protocol. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

type ControllerRegistrationRequestFeature struct {
	Name                *string // The feature name. (versions: 0+)
	MinSupportedVersion int16   // The minimum supported feature level. (versions: 0+)
	MaxSupportedVersion int16   // The maximum supported feature level. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *ControllerRegistrationRequest) Write(w io.Writer) error {
	// ControllerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.ControllerId); err != nil {
		return err
	}

	// IncarnationId (versions: 0+)
	if err := protocol.WriteUUID(w, req.IncarnationId); err != nil {
		return err
	}

	// ZkMigrationReady (versions: 0+)
	if err := protocol.WriteBool(w, req.ZkMigrationReady); err != nil {
		return err
	}

	// Listeners (versions: 0+)
	if req.Listeners == nil {
		return fmt.Errorf("ControllerRegistrationRequest.Listeners must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("ControllerRegistrationRequest.Features must not be nil in version %d", req.ApiVersion)
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
func (req *ControllerRegistrationRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("ControllerRegistrationRequest.Read: request or its body is nil")
	}

	*req = ControllerRegistrationRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ControllerId (versions: 0+)
	controllerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.ControllerId = controllerid

	// IncarnationId (versions: 0+)
	incarnationid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	req.IncarnationId = incarnationid

	// ZkMigrationReady (versions: 0+)
	zkmigrationready, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.ZkMigrationReady = zkmigrationready

	// Listeners (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		listeners, err := protocol.ReadCompactArray(r, req.listenersDecoder)
		if err != nil {
			return err
		}
		req.Listeners = &listeners
	} else {
		listeners, err := protocol.ReadArray(r, req.listenersDecoder)
		if err != nil {
			return err
		}
		req.Listeners = &listeners
	}

	// Features (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		features, err := protocol.ReadCompactArray(r, req.featuresDecoder)
		if err != nil {
			return err
		}
		req.Features = &features
	} else {
		features, err := protocol.ReadArray(r, req.featuresDecoder)
		if err != nil {
			return err
		}
		req.Features = &features
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

func (req *ControllerRegistrationRequest) listenersEncoder(w io.Writer, value ControllerRegistrationRequestListener) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ControllerRegistrationRequestListener.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("ControllerRegistrationRequestListener.Host must not be nil in version %d", req.ApiVersion)
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

func (req *ControllerRegistrationRequest) listenersDecoder(r io.Reader) (ControllerRegistrationRequestListener, error) {
	controllerregistrationrequestlistener := ControllerRegistrationRequestListener{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return controllerregistrationrequestlistener, err
		}
		controllerregistrationrequestlistener.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return controllerregistrationrequestlistener, err
		}
		controllerregistrationrequestlistener.Name = &name
	}

	// Host (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return controllerregistrationrequestlistener, err
		}
		controllerregistrationrequestlistener.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return controllerregistrationrequestlistener, err
		}
		controllerregistrationrequestlistener.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadUInt16(r)
	if err != nil {
		return controllerregistrationrequestlistener, err
	}
	controllerregistrationrequestlistener.Port = port

	// SecurityProtocol (versions: 0+)
	securityprotocol, err := protocol.ReadInt16(r)
	if err != nil {
		return controllerregistrationrequestlistener, err
	}
	controllerregistrationrequestlistener.SecurityProtocol = securityprotocol

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return controllerregistrationrequestlistener, err
		}
		controllerregistrationrequestlistener.rawTaggedFields = &rawTaggedFields
	}

	return controllerregistrationrequestlistener, nil
}

func (req *ControllerRegistrationRequest) featuresEncoder(w io.Writer, value ControllerRegistrationRequestFeature) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("ControllerRegistrationRequestFeature.Name must not be nil in version %d", req.ApiVersion)
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

func (req *ControllerRegistrationRequest) featuresDecoder(r io.Reader) (ControllerRegistrationRequestFeature, error) {
	controllerregistrationrequestfeature := ControllerRegistrationRequestFeature{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return controllerregistrationrequestfeature, err
		}
		controllerregistrationrequestfeature.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return controllerregistrationrequestfeature, err
		}
		controllerregistrationrequestfeature.Name = &name
	}

	// MinSupportedVersion (versions: 0+)
	minsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return controllerregistrationrequestfeature, err
	}
	controllerregistrationrequestfeature.MinSupportedVersion = minsupportedversion

	// MaxSupportedVersion (versions: 0+)
	maxsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return controllerregistrationrequestfeature, err
	}
	controllerregistrationrequestfeature.MaxSupportedVersion = maxsupportedversion

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return controllerregistrationrequestfeature, err
		}
		controllerregistrationrequestfeature.rawTaggedFields = &rawTaggedFields
	}

	return controllerregistrationrequestfeature, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *ControllerRegistrationRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> ControllerRegistrationRequest:\n")
	fmt.Fprintf(w, "        ControllerId: %v\n", req.ControllerId)
	fmt.Fprintf(w, "        IncarnationId: %v\n", req.IncarnationId)
	fmt.Fprintf(w, "        ZkMigrationReady: %v\n", req.ZkMigrationReady)

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

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *ControllerRegistrationRequestListener) PrettyPrint() string {
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
func (value *ControllerRegistrationRequestFeature) PrettyPrint() string {
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
