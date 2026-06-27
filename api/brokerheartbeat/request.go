package brokerheartbeat

import (
	"bytes"
	"fmt"
	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type BrokerHeartbeatRequest struct {
	ApiVersion            int16
	BrokerId              int32        // The broker ID. (versions: 0+)
	BrokerEpoch           int64        // The broker epoch. (versions: 0+)
	CurrentMetadataOffset int64        // The highest metadata offset which the broker has reached. (versions: 0+)
	WantFence             bool         // True if the broker wants to be fenced, false otherwise. (versions: 0+)
	WantShutDown          bool         // True if the broker wants to be shut down, false otherwise. (versions: 0+)
	OfflineLogDirs        *[]uuid.UUID // tag 0: Log directories that failed and went offline. (versions: 1+)
	CordonedLogDirs       *[]uuid.UUID // tag 1: List of log directories that are cordoned. This is null before the broker reaches the RECOVERY state. (versions: 2+, nullable: 2+)
	rawTaggedFields       *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *BrokerHeartbeatRequest) Write(w io.Writer) error {
	// BrokerId (versions: 0+)
	if err := protocol.WriteInt32(w, req.BrokerId); err != nil {
		return err
	}

	// BrokerEpoch (versions: 0+)
	if err := protocol.WriteInt64(w, req.BrokerEpoch); err != nil {
		return err
	}

	// CurrentMetadataOffset (versions: 0+)
	if err := protocol.WriteInt64(w, req.CurrentMetadataOffset); err != nil {
		return err
	}

	// WantFence (versions: 0+)
	if err := protocol.WriteBool(w, req.WantFence); err != nil {
		return err
	}

	// WantShutDown (versions: 0+)
	if err := protocol.WriteBool(w, req.WantShutDown); err != nil {
		return err
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		taggedFields, err := req.taggedFieldsEncoder()
		if err != nil {
			return err
		}

		if err := protocol.WriteRawTaggedFields(w, taggedFields); err != nil {
			return err
		}
	}

	return nil
}

// TODO: pass version and bytes only
func (req *BrokerHeartbeatRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("BrokerHeartbeatRequest.Read: request or its body is nil")
	}

	*req = BrokerHeartbeatRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Field defaults (applied before decode; a field absent from the wire keeps its default)
	req.BrokerEpoch = -1
	req.OfflineLogDirs = &[]uuid.UUID{}

	// BrokerId (versions: 0+)
	brokerid, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.BrokerId = brokerid

	// BrokerEpoch (versions: 0+)
	brokerepoch, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.BrokerEpoch = brokerepoch

	// CurrentMetadataOffset (versions: 0+)
	currentmetadataoffset, err := protocol.ReadInt64(r)
	if err != nil {
		return err
	}
	req.CurrentMetadataOffset = currentmetadataoffset

	// WantFence (versions: 0+)
	wantfence, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.WantFence = wantfence

	// WantShutDown (versions: 0+)
	wantshutdown, err := protocol.ReadBool(r)
	if err != nil {
		return err
	}
	req.WantShutDown = wantshutdown

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.ReadTaggedFields(r, req.taggedFieldsDecoder); err != nil {
			return err
		}
	}

	return nil
}

func (req *BrokerHeartbeatRequest) taggedFieldsEncoder() ([]protocol.TaggedField, error) {
	rawTaggedFieldsLen := 0
	if req.rawTaggedFields != nil {
		rawTaggedFieldsLen = len(*req.rawTaggedFields)
	}
	taggedFields := make([]protocol.TaggedField, 0, 2+rawTaggedFieldsLen)

	buf := bytes.NewBuffer(make([]byte, 0))

	// Tag 0
	if req.ApiVersion >= 1 && req.OfflineLogDirs != nil && len(*req.OfflineLogDirs) > 0 {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactArray(buf, protocol.WriteUUID, req.OfflineLogDirs); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 0, Field: buf.Bytes()})
	}

	// Tag 1
	if req.ApiVersion >= 2 && req.CordonedLogDirs != nil {
		buf = bytes.NewBuffer(make([]byte, 0))
		if err := protocol.WriteNullableCompactArray(buf, protocol.WriteUUID, req.CordonedLogDirs); err != nil {
			return taggedFields, err
		}

		taggedFields = append(taggedFields, protocol.TaggedField{Tag: 1, Field: buf.Bytes()})
	}

	// We append any raw tagged fields to the end of the array
	if req.rawTaggedFields != nil {
		taggedFields = append(taggedFields, *req.rawTaggedFields...)
	}

	return taggedFields, nil
}

func (req *BrokerHeartbeatRequest) taggedFieldsDecoder(r io.Reader, tag uint64, tagLength uint64) error {
	known := false

	switch tag {
	case 0:
		// OfflineLogDirs
		if req.ApiVersion >= 1 {
			known = true
			offlinelogdirs, err := protocol.ReadCompactArray(r, protocol.ReadUUID)
			if err != nil {
				return err
			}
			req.OfflineLogDirs = &offlinelogdirs
		}
	case 1:
		// CordonedLogDirs
		if req.ApiVersion >= 2 {
			known = true
			cordonedlogdirs, err := protocol.ReadNullableCompactArray(r, protocol.ReadUUID)
			if err != nil {
				return err
			}
			req.CordonedLogDirs = cordonedlogdirs
		}
	}

	if !known {
		// Keep the raw bytes (r is bounded to this tag's length by ReadTaggedFields)
		field, err := io.ReadAll(r)
		if err != nil {
			return err
		}
		if req.rawTaggedFields == nil {
			rawTaggedFields := make([]protocol.TaggedField, 0)
			req.rawTaggedFields = &rawTaggedFields
		}
		*req.rawTaggedFields = append(*req.rawTaggedFields, protocol.TaggedField{Tag: tag, Field: field})
	}

	return nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *BrokerHeartbeatRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> BrokerHeartbeatRequest:\n")
	fmt.Fprintf(w, "        BrokerId: %v\n", req.BrokerId)
	fmt.Fprintf(w, "        BrokerEpoch: %v\n", req.BrokerEpoch)
	fmt.Fprintf(w, "        CurrentMetadataOffset: %v\n", req.CurrentMetadataOffset)
	fmt.Fprintf(w, "        WantFence: %v\n", req.WantFence)
	fmt.Fprintf(w, "        WantShutDown: %v\n", req.WantShutDown)

	if req.OfflineLogDirs != nil {
		fmt.Fprintf(w, "        OfflineLogDirs: %v\n", *req.OfflineLogDirs)
	} else {
		fmt.Fprintf(w, "        OfflineLogDirs: nil\n")
	}

	if req.CordonedLogDirs != nil {
		fmt.Fprintf(w, "        CordonedLogDirs: %v\n", *req.CordonedLogDirs)
	} else {
		fmt.Fprintf(w, "        CordonedLogDirs: nil\n")
	}

	return w.String()
}
