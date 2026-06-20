package updateraftvoter

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type UpdateRaftVoterRequest struct {
	ApiVersion          int16
	ClusterId           *string                                    // The cluster id. (versions: 0+, nullable: 0+)
	CurrentLeaderEpoch  int32                                      // The current leader epoch of the partition, -1 for unknown leader epoch. (versions: 0+)
	VoterId             int32                                      // The replica id of the voter getting updated in the topic partition. (versions: 0+)
	VoterDirectoryId    uuid.UUID                                  // The directory id of the voter getting updated in the topic partition. (versions: 0+)
	Listeners           *[]UpdateRaftVoterRequestListener          // The endpoint that can be used to communicate with the leader. (versions: 0+)
	KRaftVersionFeature *UpdateRaftVoterRequestKRaftVersionFeature // The range of versions of the protocol that the replica supports. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

type UpdateRaftVoterRequestListener struct {
	Name            *string // The name of the endpoint. (versions: 0+)
	Host            *string // The hostname. (versions: 0+)
	Port            uint16  // The port. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type UpdateRaftVoterRequestKRaftVersionFeature struct {
	MinSupportedVersion int16 // The minimum supported KRaft protocol version. (versions: 0+)
	MaxSupportedVersion int16 // The maximum supported KRaft protocol version. (versions: 0+)
	rawTaggedFields     *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *UpdateRaftVoterRequest) Write(w io.Writer) error {
	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactString(w, req.ClusterId); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteNullableString(w, req.ClusterId); err != nil {
			return err
		}
	}

	// CurrentLeaderEpoch (versions: 0+)
	if err := protocol.WriteInt32(w, req.CurrentLeaderEpoch); err != nil {
		return err
	}

	// VoterId (versions: 0+)
	if err := protocol.WriteInt32(w, req.VoterId); err != nil {
		return err
	}

	// VoterDirectoryId (versions: 0+)
	if err := protocol.WriteUUID(w, req.VoterDirectoryId); err != nil {
		return err
	}

	// Listeners (versions: 0+)
	if req.Listeners == nil {
		return fmt.Errorf("UpdateRaftVoterRequest.Listeners must not be nil in version %d", req.ApiVersion)
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

	// KRaftVersionFeature (versions: 0+)
	if req.KRaftVersionFeature == nil {
		return fmt.Errorf("UpdateRaftVoterRequest.KRaftVersionFeature must not be nil in version %d", req.ApiVersion)
	}
	if err := req.kRaftVersionFeatureEncoder(w, *req.KRaftVersionFeature); err != nil {
		return err
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
func (req *UpdateRaftVoterRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("UpdateRaftVoterRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// ClusterId (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		clusterid, err := protocol.ReadNullableCompactString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	} else {
		clusterid, err := protocol.ReadNullableString(r)
		if err != nil {
			return err
		}
		req.ClusterId = clusterid
	}

	// CurrentLeaderEpoch (versions: 0+)
	currentleaderepoch, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.CurrentLeaderEpoch = currentleaderepoch

	// VoterId (versions: 0+)
	voterid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.VoterId = voterid

	// VoterDirectoryId (versions: 0+)
	voterdirectoryid, err := protocol.ReadUUID(r)
	if err != nil {
		return err
	}
	req.VoterDirectoryId = voterdirectoryid

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

	// KRaftVersionFeature (versions: 0+)
	kraftversionfeature, err := req.kRaftVersionFeatureDecoder(r)
	if err != nil {
		return err
	}
	req.KRaftVersionFeature = &kraftversionfeature

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

func (req *UpdateRaftVoterRequest) listenersEncoder(w io.Writer, value UpdateRaftVoterRequestListener) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("UpdateRaftVoterRequestListener.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("UpdateRaftVoterRequestListener.Host must not be nil in version %d", req.ApiVersion)
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

func (req *UpdateRaftVoterRequest) listenersDecoder(r io.Reader) (UpdateRaftVoterRequestListener, error) {
	updateraftvoterrequestlistener := UpdateRaftVoterRequestListener{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return updateraftvoterrequestlistener, err
		}
		updateraftvoterrequestlistener.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return updateraftvoterrequestlistener, err
		}
		updateraftvoterrequestlistener.Name = &name
	}

	// Host (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return updateraftvoterrequestlistener, err
		}
		updateraftvoterrequestlistener.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return updateraftvoterrequestlistener, err
		}
		updateraftvoterrequestlistener.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadUInt16(r)
	if err != nil {
		return updateraftvoterrequestlistener, err
	}
	updateraftvoterrequestlistener.Port = port

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return updateraftvoterrequestlistener, err
		}
		updateraftvoterrequestlistener.rawTaggedFields = &rawTaggedFields
	}

	return updateraftvoterrequestlistener, nil
}

func (req *UpdateRaftVoterRequest) kRaftVersionFeatureEncoder(w io.Writer, value UpdateRaftVoterRequestKRaftVersionFeature) error {
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

func (req *UpdateRaftVoterRequest) kRaftVersionFeatureDecoder(r io.Reader) (UpdateRaftVoterRequestKRaftVersionFeature, error) {
	updateraftvoterrequestkraftversionfeature := UpdateRaftVoterRequestKRaftVersionFeature{}

	// MinSupportedVersion (versions: 0+)
	minsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return updateraftvoterrequestkraftversionfeature, err
	}
	updateraftvoterrequestkraftversionfeature.MinSupportedVersion = minsupportedversion

	// MaxSupportedVersion (versions: 0+)
	maxsupportedversion, err := protocol.ReadInt16(r)
	if err != nil {
		return updateraftvoterrequestkraftversionfeature, err
	}
	updateraftvoterrequestkraftversionfeature.MaxSupportedVersion = maxsupportedversion

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return updateraftvoterrequestkraftversionfeature, err
		}
		updateraftvoterrequestkraftversionfeature.rawTaggedFields = &rawTaggedFields
	}

	return updateraftvoterrequestkraftversionfeature, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *UpdateRaftVoterRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> UpdateRaftVoterRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        CurrentLeaderEpoch: %v\n", req.CurrentLeaderEpoch)
	fmt.Fprintf(w, "        VoterId: %v\n", req.VoterId)
	fmt.Fprintf(w, "        VoterDirectoryId: %v\n", req.VoterDirectoryId)

	if req.Listeners != nil {
		fmt.Fprintf(w, "        Listeners:\n")
		for _, listeners := range *req.Listeners {
			fmt.Fprintf(w, "%s", listeners.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Listeners: nil\n")
	}

	fmt.Fprintf(w, "        KRaftVersionFeature:\n")
	if req.KRaftVersionFeature != nil {
		fmt.Fprintf(w, "%s", req.KRaftVersionFeature.PrettyPrint())
	} else {
		fmt.Fprintf(w, "            nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *UpdateRaftVoterRequestListener) PrettyPrint() string {
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

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *UpdateRaftVoterRequestKRaftVersionFeature) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "            MinSupportedVersion: %v\n", value.MinSupportedVersion)
	fmt.Fprintf(w, "            MaxSupportedVersion: %v\n", value.MaxSupportedVersion)

	return w.String()
}
