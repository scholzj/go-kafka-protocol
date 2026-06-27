package addraftvoter

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AddRaftVoterRequest struct {
	ApiVersion       int16
	ClusterId        *string                        // The cluster id. (versions: 0+, nullable: 0+)
	TimeoutMs        int32                          // The maximum time to wait for the request to complete before returning. (versions: 0+)
	VoterId          int32                          // The replica id of the voter getting added to the topic partition. (versions: 0+)
	VoterDirectoryId uuid.UUID                      // The directory id of the voter getting added to the topic partition. (versions: 0+)
	Listeners        *[]AddRaftVoterRequestListener // The endpoints that can be used to communicate with the voter. (versions: 0+)
	AckWhenCommitted bool                           // When true, return a response after the new voter set is committed. Otherwise, return after the leader writes the changes locally. (versions: 1+)
	rawTaggedFields  *[]protocol.TaggedField
}

type AddRaftVoterRequestListener struct {
	Name            *string // The name of the endpoint. (versions: 0+)
	Host            *string // The hostname. (versions: 0+)
	Port            uint16  // The port. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *AddRaftVoterRequest) Write(w io.Writer) error {
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

	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
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
		return fmt.Errorf("AddRaftVoterRequest.Listeners must not be nil in version %d", req.ApiVersion)
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

	// AckWhenCommitted (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.AckWhenCommitted); err != nil {
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
func (req *AddRaftVoterRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AddRaftVoterRequest.Read: request or its body is nil")
	}

	*req = AddRaftVoterRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.AckWhenCommitted = true

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

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

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

	// AckWhenCommitted (versions: 1+)
	if req.ApiVersion >= 1 {
		ackwhencommitted, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.AckWhenCommitted = ackwhencommitted
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

func (req *AddRaftVoterRequest) listenersEncoder(w io.Writer, value AddRaftVoterRequestListener) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AddRaftVoterRequestListener.Name must not be nil in version %d", req.ApiVersion)
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
		return fmt.Errorf("AddRaftVoterRequestListener.Host must not be nil in version %d", req.ApiVersion)
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

func (req *AddRaftVoterRequest) listenersDecoder(r io.Reader) (AddRaftVoterRequestListener, error) {
	addraftvoterrequestlistener := AddRaftVoterRequestListener{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return addraftvoterrequestlistener, err
		}
		addraftvoterrequestlistener.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return addraftvoterrequestlistener, err
		}
		addraftvoterrequestlistener.Name = &name
	}

	// Host (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		host, err := protocol.ReadCompactString(r)
		if err != nil {
			return addraftvoterrequestlistener, err
		}
		addraftvoterrequestlistener.Host = &host
	} else {
		host, err := protocol.ReadString(r)
		if err != nil {
			return addraftvoterrequestlistener, err
		}
		addraftvoterrequestlistener.Host = &host
	}

	// Port (versions: 0+)
	port, err := protocol.ReadUInt16(r)
	if err != nil {
		return addraftvoterrequestlistener, err
	}
	addraftvoterrequestlistener.Port = port

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return addraftvoterrequestlistener, err
		}
		addraftvoterrequestlistener.rawTaggedFields = &rawTaggedFields
	}

	return addraftvoterrequestlistener, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AddRaftVoterRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AddRaftVoterRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)
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

	fmt.Fprintf(w, "        AckWhenCommitted: %v\n", req.AckWhenCommitted)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AddRaftVoterRequestListener) PrettyPrint() string {
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
