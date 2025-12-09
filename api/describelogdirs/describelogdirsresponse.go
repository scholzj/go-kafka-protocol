package describelogdirs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeLogDirsResponseApiKey        = 35
	DescribeLogDirsResponseHeaderVersion = 1
)

// DescribeLogDirsResponse represents a response message.
type DescribeLogDirsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"3-999"`
	// The log directories.
	Results []DescribeLogDirsResponseDescribeLogDirsResult `json:"results" versions:"0-999"`
}

// Encode encodes a DescribeLogDirsResponse to a byte slice for the given version.
func (m *DescribeLogDirsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeLogDirsResponse from a byte slice for the given version.
func (m *DescribeLogDirsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeLogDirsResponse to an io.Writer for the given version.
func (m *DescribeLogDirsResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 3 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Results
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Results) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Results))); err != nil {
				return err
			}
		}
		for i := range m.Results {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Results[i].ErrorCode); err != nil {
					return err
				}
			}
			// LogDir
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Results[i].LogDir); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Results[i].LogDir); err != nil {
						return err
					}
				}
			}
			// Topics
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Results[i].Topics) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Results[i].Topics))); err != nil {
						return err
					}
				}
				for i := range m.Results[i].Topics {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Results[i].Topics[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Results[i].Topics[i].Name); err != nil {
								return err
							}
						}
					}
					// Partitions
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Results[i].Topics[i].Partitions) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Results[i].Topics[i].Partitions))); err != nil {
								return err
							}
						}
						for i := range m.Results[i].Topics[i].Partitions {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Results[i].Topics[i].Partitions[i].PartitionIndex); err != nil {
									return err
								}
							}
							// PartitionSize
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Results[i].Topics[i].Partitions[i].PartitionSize); err != nil {
									return err
								}
							}
							// OffsetLag
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Results[i].Topics[i].Partitions[i].OffsetLag); err != nil {
									return err
								}
							}
							// IsFutureKey
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(w, m.Results[i].Topics[i].Partitions[i].IsFutureKey); err != nil {
									return err
								}
							}
						}
					}
				}
			}
			// TotalBytes
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Results[i].TotalBytes); err != nil {
					return err
				}
			}
			// UsableBytes
			if version >= 4 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Results[i].UsableBytes); err != nil {
					return err
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

// Read reads a DescribeLogDirsResponse from an io.Reader for the given version.
func (m *DescribeLogDirsResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 3 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Results
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
			m.Results = make([]DescribeLogDirsResponseDescribeLogDirsResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].LogDir = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].LogDir = val
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
						m.Results[i].Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
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
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
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
						m.Results[i].Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
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
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								}
							}
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Results[i].TotalBytes = val
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Results[i].UsableBytes = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Results = make([]DescribeLogDirsResponseDescribeLogDirsResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// LogDir
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].LogDir = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].LogDir = val
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
						m.Results[i].Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
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
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
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
						m.Results[i].Topics = make([]DescribeLogDirsResponseDescribeLogDirsTopic, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Name = val
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
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Topics[i].Partitions = make([]DescribeLogDirsResponseDescribeLogDirsPartition, length)
									for i := int32(0); i < length; i++ {
										// PartitionIndex
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionIndex = val
										}
										// PartitionSize
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].PartitionSize = val
										}
										// OffsetLag
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].OffsetLag = val
										}
										// IsFutureKey
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadBool(r)
											if err != nil {
												return err
											}
											m.Results[i].Topics[i].Partitions[i].IsFutureKey = val
										}
									}
								}
							}
						}
					}
				}
				// TotalBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Results[i].TotalBytes = val
				}
				// UsableBytes
				if version >= 4 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Results[i].UsableBytes = val
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

// DescribeLogDirsResponseDescribeLogDirsResult represents The log directories..
type DescribeLogDirsResponseDescribeLogDirsResult struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The absolute log directory path.
	LogDir string `json:"logdir" versions:"0-999"`
	// The topics.
	Topics []DescribeLogDirsResponseDescribeLogDirsTopic `json:"topics" versions:"0-999"`
	// The total size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
	TotalBytes int64 `json:"totalbytes" versions:"4-999"`
	// The usable size in bytes of the volume the log directory is in. This value does not include the size of data stored in remote storage.
	UsableBytes int64 `json:"usablebytes" versions:"4-999"`
}

// DescribeLogDirsResponseDescribeLogDirsTopic represents The topics..
type DescribeLogDirsResponseDescribeLogDirsTopic struct {
	// The topic name.
	Name string `json:"name" versions:"0-999"`
	// The partitions.
	Partitions []DescribeLogDirsResponseDescribeLogDirsPartition `json:"partitions" versions:"0-999"`
}

// DescribeLogDirsResponseDescribeLogDirsPartition represents The partitions..
type DescribeLogDirsResponseDescribeLogDirsPartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The size of the log segments in this partition in bytes.
	PartitionSize int64 `json:"partitionsize" versions:"0-999"`
	// The lag of the log's LEO w.r.t. partition's HW (if it is the current log for the partition) or current replica's LEO (if it is the future log for the partition).
	OffsetLag int64 `json:"offsetlag" versions:"0-999"`
	// True if this log is created by AlterReplicaLogDirsRequest and will replace the current log of the replica in the future.
	IsFutureKey bool `json:"isfuturekey" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeLogDirsResponse.
func (m *DescribeLogDirsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeLogDirsResponse.
func (m *DescribeLogDirsResponse) readTaggedFields(r io.Reader, version int16) error {
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
