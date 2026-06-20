package updatefeatures

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type UpdateFeaturesRequest struct {
	ApiVersion      int16
	TimeoutMs       int32                                 // How long to wait in milliseconds before timing out the request. (versions: 0+)
	FeatureUpdates  *[]UpdateFeaturesRequestFeatureUpdate // The list of updates to finalized features. (versions: 0+)
	ValidateOnly    bool                                  // True if we should validate the request, but not perform the upgrade or downgrade. (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

type UpdateFeaturesRequestFeatureUpdate struct {
	Feature         *string // The name of the finalized feature to be updated. (versions: 0+)
	MaxVersionLevel int16   // The new maximum version level for the finalized feature. A value >= 1 is valid. A value < 1, is special, and can be used to request the deletion of the finalized feature. (versions: 0+)
	AllowDowngrade  bool    // DEPRECATED in version 1 (see DowngradeType). When set to true, the finalized feature version level is allowed to be downgraded/deleted. The downgrade request will fail if the new maximum version level is a value that's not lower than the existing maximum finalized version level. (versions: 0)
	UpgradeType     int8    // Determine which type of upgrade will be performed: 1 will perform an upgrade only (default), 2 is safe downgrades only (lossless), 3 is unsafe downgrades (lossy). (versions: 1+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 0
}

func (req *UpdateFeaturesRequest) Write(w io.Writer) error {
	// TimeoutMs (versions: 0+)
	if err := protocol.WriteInt32(w, req.TimeoutMs); err != nil {
		return err
	}

	// FeatureUpdates (versions: 0+)
	if req.FeatureUpdates == nil {
		return fmt.Errorf("UpdateFeaturesRequest.FeatureUpdates must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.featureUpdatesEncoder, req.FeatureUpdates); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.featureUpdatesEncoder, *req.FeatureUpdates); err != nil {
			return err
		}
	}

	// ValidateOnly (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteBool(w, req.ValidateOnly); err != nil {
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
func (req *UpdateFeaturesRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("UpdateFeaturesRequest.Read: request or its body is nil")
	}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// TimeoutMs (versions: 0+)
	timeoutms, err := protocol.ReadInt32(r)
	if err != nil {
		return err
	}
	req.TimeoutMs = timeoutms

	// FeatureUpdates (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		featureupdates, err := protocol.ReadNullableCompactArray(r, req.featureUpdatesDecoder)
		if err != nil {
			return err
		}
		req.FeatureUpdates = featureupdates
	} else {
		featureupdates, err := protocol.ReadArray(r, req.featureUpdatesDecoder)
		if err != nil {
			return err
		}
		req.FeatureUpdates = &featureupdates
	}

	// ValidateOnly (versions: 1+)
	if req.ApiVersion >= 1 {
		validateonly, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		req.ValidateOnly = validateonly
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

func (req *UpdateFeaturesRequest) featureUpdatesEncoder(w io.Writer, value UpdateFeaturesRequestFeatureUpdate) error {
	// Feature (versions: 0+)
	if value.Feature == nil {
		return fmt.Errorf("UpdateFeaturesRequestFeatureUpdate.Feature must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Feature); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Feature); err != nil {
			return err
		}
	}

	// MaxVersionLevel (versions: 0+)
	if err := protocol.WriteInt16(w, value.MaxVersionLevel); err != nil {
		return err
	}

	// AllowDowngrade (versions: 0)
	if req.ApiVersion == 0 {
		if err := protocol.WriteBool(w, value.AllowDowngrade); err != nil {
			return err
		}
	}

	// UpgradeType (versions: 1+)
	if req.ApiVersion >= 1 {
		if err := protocol.WriteInt8(w, value.UpgradeType); err != nil {
			return err
		}
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

func (req *UpdateFeaturesRequest) featureUpdatesDecoder(r io.Reader) (UpdateFeaturesRequestFeatureUpdate, error) {
	updatefeaturesrequestfeatureupdate := UpdateFeaturesRequestFeatureUpdate{}

	// Feature (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		feature, err := protocol.ReadCompactString(r)
		if err != nil {
			return updatefeaturesrequestfeatureupdate, err
		}
		updatefeaturesrequestfeatureupdate.Feature = &feature
	} else {
		feature, err := protocol.ReadString(r)
		if err != nil {
			return updatefeaturesrequestfeatureupdate, err
		}
		updatefeaturesrequestfeatureupdate.Feature = &feature
	}

	// MaxVersionLevel (versions: 0+)
	maxversionlevel, err := protocol.ReadInt16(r)
	if err != nil {
		return updatefeaturesrequestfeatureupdate, err
	}
	updatefeaturesrequestfeatureupdate.MaxVersionLevel = maxversionlevel

	// AllowDowngrade (versions: 0)
	if req.ApiVersion == 0 {
		allowdowngrade, err := protocol.ReadBool(r)
		if err != nil {
			return updatefeaturesrequestfeatureupdate, err
		}
		updatefeaturesrequestfeatureupdate.AllowDowngrade = allowdowngrade
	}

	// UpgradeType (versions: 1+)
	if req.ApiVersion >= 1 {
		upgradetype, err := protocol.ReadInt8(r)
		if err != nil {
			return updatefeaturesrequestfeatureupdate, err
		}
		updatefeaturesrequestfeatureupdate.UpgradeType = upgradetype
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return updatefeaturesrequestfeatureupdate, err
		}
		updatefeaturesrequestfeatureupdate.rawTaggedFields = &rawTaggedFields
	}

	return updatefeaturesrequestfeatureupdate, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *UpdateFeaturesRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> UpdateFeaturesRequest:\n")
	fmt.Fprintf(w, "        TimeoutMs: %v\n", req.TimeoutMs)

	if req.FeatureUpdates != nil {
		fmt.Fprintf(w, "        FeatureUpdates:\n")
		for _, featureupdates := range *req.FeatureUpdates {
			fmt.Fprintf(w, "%s", featureupdates.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        FeatureUpdates: nil\n")
	}

	fmt.Fprintf(w, "        ValidateOnly: %v\n", req.ValidateOnly)

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *UpdateFeaturesRequestFeatureUpdate) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Feature != nil {
		fmt.Fprintf(w, "            Feature: %v\n", *value.Feature)
	} else {
		fmt.Fprintf(w, "            Feature: nil\n")
	}

	fmt.Fprintf(w, "            MaxVersionLevel: %v\n", value.MaxVersionLevel)
	fmt.Fprintf(w, "            AllowDowngrade: %v\n", value.AllowDowngrade)
	fmt.Fprintf(w, "            UpgradeType: %v\n", value.UpgradeType)

	return w.String()
}
