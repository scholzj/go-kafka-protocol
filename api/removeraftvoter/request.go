package removeraftvoter

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type RemoveRaftVoterRequest struct {
	ApiVersion       int16
	ClusterId        *string   // The cluster id of the request. (versions: 0+, nullable: 0+)
	VoterId          int32     // The replica id of the voter getting removed from the topic partition. (versions: 0+)
	VoterDirectoryId uuid.UUID // The directory id of the voter getting removed from the topic partition. (versions: 0+)
	rawTaggedFields  *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *RemoveRaftVoterRequest) Write(w io.Writer) error {
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

	// VoterId (versions: 0+)
	if err := protocol.WriteInt32(w, req.VoterId); err != nil {
		return err
	}

	// VoterDirectoryId (versions: 0+)
	if err := protocol.WriteUUID(w, req.VoterDirectoryId); err != nil {
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
func (req *RemoveRaftVoterRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("RemoveRaftVoterRequest.Read: request or its body is nil")
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
func (req *RemoveRaftVoterRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> RemoveRaftVoterRequest:\n")

	if req.ClusterId != nil {
		fmt.Fprintf(w, "        ClusterId: %v\n", *req.ClusterId)
	} else {
		fmt.Fprintf(w, "        ClusterId: nil\n")
	}

	fmt.Fprintf(w, "        VoterId: %v\n", req.VoterId)
	fmt.Fprintf(w, "        VoterDirectoryId: %v\n", req.VoterDirectoryId)

	return w.String()
}
