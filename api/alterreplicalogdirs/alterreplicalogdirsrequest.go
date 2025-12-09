package alterreplicalogdirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterReplicaLogDirsRequestApiKey        = 34
	AlterReplicaLogDirsRequestHeaderVersion = 1
)

// AlterReplicaLogDirsRequest represents a request message.
type AlterReplicaLogDirsRequest struct {
	// The alterations to make for each directory.
	Dirs []AlterReplicaLogDirsRequestAlterReplicaLogDir `json:"dirs" versions:"0-999"`
}

// Encode encodes a AlterReplicaLogDirsRequest to a byte slice for the given version.
func (m *AlterReplicaLogDirsRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterReplicaLogDirsRequest from a byte slice for the given version.
func (m *AlterReplicaLogDirsRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterReplicaLogDirsRequest to an io.Writer for the given version.
func (m *AlterReplicaLogDirsRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Dirs
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Dirs) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Dirs))); err != nil {
				return err
			}
		}
		for i := range m.Dirs {
			// Path
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Dirs[i].Path); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Dirs[i].Path); err != nil {
						return err
					}
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Dirs[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Dirs[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.Dirs[i].Topics {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Dirs[i].Topics[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Dirs[i].Topics[i].Name); err != nil {
								return err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Dirs[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Dirs[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.Dirs[i].Topics[i].Partitions {
							if err := protocol.WriteInt32(w, m.Dirs[i].Topics[i].Partitions[i]); err != nil {
								return err
							}
							_ = i
						}
					}
				}
			}
		}
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a AlterReplicaLogDirsRequest from an io.Reader for the given version.
func (m *AlterReplicaLogDirsRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// Dirs
	if version >= 0 && version <= 999 {
		var length int32
		if isFlexible {
			var lengthUint uint32
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length = int32(lengthUint - 1)
			m.Dirs = make([]AlterReplicaLogDirsRequestAlterReplicaLogDir, length)
			for i := int32(0); i < length; i++ {
				// Path
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Path = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Path = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Dirs[i].Topics = make([]AlterReplicaLogDirsRequestAlterReplicaLogDirTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length = int32(lengthUint - 1)
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Topics = make([]AlterReplicaLogDirsRequestAlterReplicaLogDirTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length = int32(lengthUint - 1)
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								}
							}
						}
					}
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Dirs = make([]AlterReplicaLogDirsRequestAlterReplicaLogDir, length)
			for i := int32(0); i < length; i++ {
				// Path
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Path = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Path = val
					}
				}
				// Topics
				if version >= 0 && version <= 999 {
					var length int32
					if isFlexible {
						var lengthUint uint32
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length = int32(lengthUint - 1)
						m.Dirs[i].Topics = make([]AlterReplicaLogDirsRequestAlterReplicaLogDirTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length = int32(lengthUint - 1)
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Dirs[i].Topics = make([]AlterReplicaLogDirsRequestAlterReplicaLogDirTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Name = val
								}
							}
							// Partitions
							if version >= 0 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length = int32(lengthUint - 1)
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Dirs[i].Topics[i].Partitions = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Dirs[i].Topics[i].Partitions[i] = val
									}
								}
							}
						}
					}
				}
			}
		}
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// AlterReplicaLogDirsRequestAlterReplicaLogDir represents The alterations to make for each directory..
type AlterReplicaLogDirsRequestAlterReplicaLogDir struct {
	// The absolute directory path.
	Path string `json:"path" versions:"0-999"`
	// The topics to add to the directory.
	Topics []AlterReplicaLogDirsRequestAlterReplicaLogDirTopic `json:"topics" versions:"0-999"`
}

// AlterReplicaLogDirsRequestAlterReplicaLogDirTopic represents The topics to add to the directory..
type AlterReplicaLogDirsRequestAlterReplicaLogDirTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partition indexes.
	Partitions []int32 `json:"partitions" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for AlterReplicaLogDirsRequest.
func (m *AlterReplicaLogDirsRequest) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	// Write tagged fields count
	if err := protocol.WriteVaruint32(w, uint32(taggedFieldsCount)); err != nil {
		return err
	}

	// Write tagged fields data
	if taggedFieldsCount > 0 {
		if _, err := w.Write(taggedFieldsBuf.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

// readTaggedFields reads tagged fields for AlterReplicaLogDirsRequest.
func (m *AlterReplicaLogDirsRequest) readTaggedFields(r io.Reader, version int16) error {
	// Read tagged fields count
	count, err := protocol.ReadVaruint32(r)
	if err != nil {
		return err
	}

	if count == 0 {
		return nil
	}

	// Read tagged fields
	for i := uint32(0); i < count; i++ {
		tag, err := protocol.ReadVaruint32(r)
		if err != nil {
			return err
		}

		switch tag {
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
