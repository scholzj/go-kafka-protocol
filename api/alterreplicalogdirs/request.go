package alterreplicalogdirs

import (
	"bytes"
	"fmt"
	"github.com/scholzj/go-kafka-protocol/protocol"
	"io"
)

type AlterReplicaLogDirsRequest struct {
	ApiVersion      int16
	Dirs            *[]AlterReplicaLogDirsRequestDir // The alterations to make for each directory. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterReplicaLogDirsRequestDir struct {
	Path            *string                               // The absolute directory path. (versions: 0+)
	Topics          *[]AlterReplicaLogDirsRequestDirTopic // The topics to add to the directory. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

type AlterReplicaLogDirsRequestDirTopic struct {
	Name            *string  // The topic name. (versions: 0+)
	Partitions      *[]int32 // The partition indexes. (versions: 0+)
	rawTaggedFields *[]protocol.TaggedField
}

func isRequestFlexible(apiVersion int16) bool {
	return apiVersion >= 2
}

func (req *AlterReplicaLogDirsRequest) Write(w io.Writer) error {
	// Dirs (versions: 0+)
	if req.Dirs == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequest.Dirs must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.dirsEncoder, req.Dirs); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.dirsEncoder, *req.Dirs); err != nil {
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
func (req *AlterReplicaLogDirsRequest) Read(request *protocol.Request) error {
	if request == nil || request.Body == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequest.Read: request or its body is nil")
	}

	*req = AlterReplicaLogDirsRequest{}

	r := bytes.NewBuffer(request.Body.Bytes())
	req.ApiVersion = request.ApiVersion

	// Dirs (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		dirs, err := protocol.ReadCompactArray(r, req.dirsDecoder)
		if err != nil {
			return err
		}
		req.Dirs = &dirs
	} else {
		dirs, err := protocol.ReadArray(r, req.dirsDecoder)
		if err != nil {
			return err
		}
		req.Dirs = &dirs
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

func (req *AlterReplicaLogDirsRequest) dirsEncoder(w io.Writer, value AlterReplicaLogDirsRequestDir) error {
	// Path (versions: 0+)
	if value.Path == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequestDir.Path must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteCompactString(w, *value.Path); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteString(w, *value.Path); err != nil {
			return err
		}
	}

	// Topics (versions: 0+)
	if value.Topics == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequestDir.Topics must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, req.topicsEncoder, value.Topics); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, req.topicsEncoder, *value.Topics); err != nil {
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

func (req *AlterReplicaLogDirsRequest) dirsDecoder(r io.Reader) (AlterReplicaLogDirsRequestDir, error) {
	alterreplicalogdirsrequestdir := AlterReplicaLogDirsRequestDir{}

	// Path (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		path, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterreplicalogdirsrequestdir, err
		}
		alterreplicalogdirsrequestdir.Path = &path
	} else {
		path, err := protocol.ReadString(r)
		if err != nil {
			return alterreplicalogdirsrequestdir, err
		}
		alterreplicalogdirsrequestdir.Path = &path
	}

	// Topics (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		topics, err := protocol.ReadCompactArray(r, req.topicsDecoder)
		if err != nil {
			return alterreplicalogdirsrequestdir, err
		}
		alterreplicalogdirsrequestdir.Topics = &topics
	} else {
		topics, err := protocol.ReadArray(r, req.topicsDecoder)
		if err != nil {
			return alterreplicalogdirsrequestdir, err
		}
		alterreplicalogdirsrequestdir.Topics = &topics
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterreplicalogdirsrequestdir, err
		}
		alterreplicalogdirsrequestdir.rawTaggedFields = &rawTaggedFields
	}

	return alterreplicalogdirsrequestdir, nil
}

func (req *AlterReplicaLogDirsRequest) topicsEncoder(w io.Writer, value AlterReplicaLogDirsRequestDirTopic) error {
	// Name (versions: 0+)
	if value.Name == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequestDirTopic.Name must not be nil in version %d", req.ApiVersion)
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

	// Partitions (versions: 0+)
	if value.Partitions == nil {
		return fmt.Errorf("AlterReplicaLogDirsRequestDirTopic.Partitions must not be nil in version %d", req.ApiVersion)
	}
	if isRequestFlexible(req.ApiVersion) {
		if err := protocol.WriteNullableCompactArray(w, protocol.WriteInt32, value.Partitions); err != nil {
			return err
		}
	} else {
		if err := protocol.WriteArray(w, protocol.WriteInt32, *value.Partitions); err != nil {
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

func (req *AlterReplicaLogDirsRequest) topicsDecoder(r io.Reader) (AlterReplicaLogDirsRequestDirTopic, error) {
	alterreplicalogdirsrequestdirtopic := AlterReplicaLogDirsRequestDirTopic{}

	// Name (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		name, err := protocol.ReadCompactString(r)
		if err != nil {
			return alterreplicalogdirsrequestdirtopic, err
		}
		alterreplicalogdirsrequestdirtopic.Name = &name
	} else {
		name, err := protocol.ReadString(r)
		if err != nil {
			return alterreplicalogdirsrequestdirtopic, err
		}
		alterreplicalogdirsrequestdirtopic.Name = &name
	}

	// Partitions (versions: 0+)
	if isRequestFlexible(req.ApiVersion) {
		partitions, err := protocol.ReadCompactArray(r, protocol.ReadInt32)
		if err != nil {
			return alterreplicalogdirsrequestdirtopic, err
		}
		alterreplicalogdirsrequestdirtopic.Partitions = &partitions
	} else {
		partitions, err := protocol.ReadArray(r, protocol.ReadInt32)
		if err != nil {
			return alterreplicalogdirsrequestdirtopic, err
		}
		alterreplicalogdirsrequestdirtopic.Partitions = &partitions
	}

	// Tagged fields
	if isRequestFlexible(req.ApiVersion) {
		rawTaggedFields, err := protocol.ReadRawTaggedFields(r)
		if err != nil {
			return alterreplicalogdirsrequestdirtopic, err
		}
		alterreplicalogdirsrequestdirtopic.rawTaggedFields = &rawTaggedFields
	}

	return alterreplicalogdirsrequestdirtopic, nil
}

//goland:noinspection GoUnhandledErrorResult
func (req *AlterReplicaLogDirsRequest) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	fmt.Fprintf(w, "    -> AlterReplicaLogDirsRequest:\n")

	if req.Dirs != nil {
		fmt.Fprintf(w, "        Dirs:\n")
		for _, dirs := range *req.Dirs {
			fmt.Fprintf(w, "%s", dirs.PrettyPrint())
			fmt.Fprintf(w, "            ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "        Dirs: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterReplicaLogDirsRequestDir) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Path != nil {
		fmt.Fprintf(w, "            Path: %v\n", *value.Path)
	} else {
		fmt.Fprintf(w, "            Path: nil\n")
	}

	if value.Topics != nil {
		fmt.Fprintf(w, "            Topics:\n")
		for _, topics := range *value.Topics {
			fmt.Fprintf(w, "%s", topics.PrettyPrint())
			fmt.Fprintf(w, "                ----------------\n")
		}
	} else {
		fmt.Fprintf(w, "            Topics: nil\n")
	}

	return w.String()
}

//goland:noinspection GoUnhandledErrorResult
func (value *AlterReplicaLogDirsRequestDirTopic) PrettyPrint() string {
	w := bytes.NewBuffer([]byte{})

	if value.Name != nil {
		fmt.Fprintf(w, "                Name: %v\n", *value.Name)
	} else {
		fmt.Fprintf(w, "                Name: nil\n")
	}

	if value.Partitions != nil {
		fmt.Fprintf(w, "                Partitions: %v\n", *value.Partitions)
	} else {
		fmt.Fprintf(w, "                Partitions: nil\n")
	}

	return w.String()
}
