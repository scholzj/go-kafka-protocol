package endquorumepoch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	EndQuorumEpochRequestApiKey        = 54
	EndQuorumEpochRequestHeaderVersion = 1
)

// EndQuorumEpochRequest represents a request message.
type EndQuorumEpochRequest struct {
	// The cluster id.
	ClusterId *string `json:"clusterid" versions:"0-999"`
	// The topics.
	Topics []EndQuorumEpochRequestTopicData `json:"topics" versions:"0-999"`
	// Endpoints for the leader.
	LeaderEndpoints []EndQuorumEpochRequestLeaderEndpoint `json:"leaderendpoints" versions:"1-999"`
}

// Encode encodes a EndQuorumEpochRequest to a byte slice for the given version.
func (m *EndQuorumEpochRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a EndQuorumEpochRequest from a byte slice for the given version.
func (m *EndQuorumEpochRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a EndQuorumEpochRequest to an io.Writer for the given version.
func (m *EndQuorumEpochRequest) Write(w io.Writer, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.ClusterId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.ClusterId); err != nil {
				return err
			}
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(EndQuorumEpochRequestTopicData)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// TopicName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.TopicName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.TopicName); err != nil {
						return nil, err
					}
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Partitions) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PartitionIndex); err != nil {
							return nil, err
						}
					}
					// LeaderId
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderId); err != nil {
							return nil, err
						}
					}
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Partitions[i].LeaderEpoch); err != nil {
							return nil, err
						}
					}
					// PreferredSuccessors
					if version >= 0 && version <= 0 {
						if isFlexible {
							if err := protocol.WriteCompactInt32Array(elemW, structItem.Partitions[i].PreferredSuccessors); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32Array(elemW, structItem.Partitions[i].PreferredSuccessors); err != nil {
								return nil, err
							}
						}
					}
					// PreferredCandidates
					if version >= 1 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Partitions[i].PreferredCandidates) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Partitions[i].PreferredCandidates))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Partitions[i].PreferredCandidates {
							// CandidateId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Partitions[i].PreferredCandidates[i].CandidateId); err != nil {
									return nil, err
								}
							}
							// CandidateDirectoryId
							if version >= 1 && version <= 999 {
								if err := protocol.WriteUUID(elemW, structItem.Partitions[i].PreferredCandidates[i].CandidateDirectoryId); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.Topics))
		for i := range m.Topics {
			items[i] = m.Topics[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
			}
		}
	}
	// LeaderEndpoints
	if version >= 1 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(EndQuorumEpochRequestLeaderEndpoint)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// Name
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Name); err != nil {
						return nil, err
					}
				}
			}
			// Host
			if version >= 1 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.Host); err != nil {
						return nil, err
					}
				}
			}
			// Port
			if version >= 1 && version <= 999 {
			}
			// Write tagged fields if flexible
			if isFlexible {
				if err := structItem.writeTaggedFields(elemW, version); err != nil {
					return nil, err
				}
			}
			return elemBuf.Bytes(), nil
		}
		items := make([]interface{}, len(m.LeaderEndpoints))
		for i := range m.LeaderEndpoints {
			items[i] = m.LeaderEndpoints[i]
		}
		if isFlexible {
			if err := protocol.WriteCompactArray(w, items, encoder); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteArray(w, items, encoder); err != nil {
				return err
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

// Read reads a EndQuorumEpochRequest from an io.Reader for the given version.
func (m *EndQuorumEpochRequest) Read(r io.Reader, version int16) error {
	if version < 0 || version > 1 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 1 {
		isFlexible = true
	}

	// ClusterId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.ClusterId = val
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem EndQuorumEpochRequestTopicData
			elemR := bytes.NewReader(data)
			// TopicName
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TopicName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.TopicName = val
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem EndQuorumEpochRequestTopicData
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TopicName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TopicName = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem EndQuorumEpochRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// PreferredSuccessors
						if version >= 0 && version <= 0 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PreferredSuccessors = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PreferredSuccessors = val
							}
						}
						// PreferredCandidates
						if version >= 1 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length := int32(lengthUint - 1)
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem EndQuorumEpochRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem EndQuorumEpochRequestReplicaInfo
									elemR := bytes.NewReader(data)
									// CandidateId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateId = val
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateDirectoryId = val
									}
									consumed := len(data) - elemR.Len()
									return elem, consumed, nil
								}
								if isFlexible {
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length := int32(lengthUint - 1)
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeCompactArray
									lengthBytes := protocol.EncodeVaruint32(lengthUint)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeArray
									lengthBytes := protocol.EncodeInt32(length)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.PreferredCandidates) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.PreferredCandidates))); err != nil {
										return err
									}
								}
								for i := range tempElem.PreferredCandidates {
									// CandidateId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.PreferredCandidates[i].CandidateId); err != nil {
											return err
										}
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.PreferredCandidates[i].CandidateDirectoryId); err != nil {
											return err
										}
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeCompactArray
						lengthBytes := protocol.EncodeVaruint32(lengthUint)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Partitions = make([]EndQuorumEpochRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(EndQuorumEpochRequestPartitionData)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem EndQuorumEpochRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem EndQuorumEpochRequestReplicaInfo
									elemR := bytes.NewReader(data)
									// CandidateId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateId = val
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateDirectoryId = val
									}
									consumed := len(data) - elemR.Len()
									return elem, consumed, nil
								}
								if isFlexible {
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length := int32(lengthUint - 1)
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeCompactArray
									lengthBytes := protocol.EncodeVaruint32(lengthUint)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeArray
									lengthBytes := protocol.EncodeInt32(length)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.PreferredCandidates) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.PreferredCandidates))); err != nil {
										return err
									}
								}
								for i := range tempElem.PreferredCandidates {
									// CandidateId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.PreferredCandidates[i].CandidateId); err != nil {
											return err
										}
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.PreferredCandidates[i].CandidateDirectoryId); err != nil {
											return err
										}
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeArray
						lengthBytes := protocol.EncodeInt32(length)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Partitions = make([]EndQuorumEpochRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(EndQuorumEpochRequestPartitionData)
						}
					}
				}
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// PreferredSuccessors
						if version >= 0 && version <= 0 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].PreferredSuccessors); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].PreferredSuccessors); err != nil {
									return err
								}
							}
						}
						// PreferredCandidates
						if version >= 1 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].PreferredCandidates) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].PreferredCandidates))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].PreferredCandidates {
								// CandidateId
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PreferredCandidates[i].CandidateId); err != nil {
										return err
									}
								}
								// CandidateDirectoryId
								if version >= 1 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].PreferredCandidates[i].CandidateDirectoryId); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Topics = make([]EndQuorumEpochRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(EndQuorumEpochRequestTopicData)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem EndQuorumEpochRequestTopicData
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.TopicName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.TopicName = val
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem EndQuorumEpochRequestPartitionData
						elemR := bytes.NewReader(data)
						// PartitionIndex
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.PartitionIndex = val
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderId = val
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.LeaderEpoch = val
						}
						// PreferredSuccessors
						if version >= 0 && version <= 0 {
							if isFlexible {
								val, err := protocol.ReadCompactInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PreferredSuccessors = val
							} else {
								val, err := protocol.ReadInt32Array(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.PreferredSuccessors = val
							}
						}
						// PreferredCandidates
						if version >= 1 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						consumed := len(data) - elemR.Len()
						return elem, consumed, nil
					}
					if isFlexible {
						lengthUint, err := protocol.ReadVaruint32(r)
						if err != nil {
							return err
						}
						if lengthUint < 1 {
							return errors.New("invalid compact array length")
						}
						length := int32(lengthUint - 1)
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem EndQuorumEpochRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem EndQuorumEpochRequestReplicaInfo
									elemR := bytes.NewReader(data)
									// CandidateId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateId = val
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateDirectoryId = val
									}
									consumed := len(data) - elemR.Len()
									return elem, consumed, nil
								}
								if isFlexible {
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length := int32(lengthUint - 1)
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeCompactArray
									lengthBytes := protocol.EncodeVaruint32(lengthUint)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeArray
									lengthBytes := protocol.EncodeInt32(length)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.PreferredCandidates) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.PreferredCandidates))); err != nil {
										return err
									}
								}
								for i := range tempElem.PreferredCandidates {
									// CandidateId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.PreferredCandidates[i].CandidateId); err != nil {
											return err
										}
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.PreferredCandidates[i].CandidateDirectoryId); err != nil {
											return err
										}
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeCompactArray
						lengthBytes := protocol.EncodeVaruint32(lengthUint)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Partitions = make([]EndQuorumEpochRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(EndQuorumEpochRequestPartitionData)
						}
					} else {
						length, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						// Collect all array elements into a buffer
						var arrayBuf bytes.Buffer
						for i := int32(0); i < length; i++ {
							// Read element into struct and encode to buffer
							var elemBuf bytes.Buffer
							elemW := &elemBuf
							var tempElem EndQuorumEpochRequestPartitionData
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.PartitionIndex = val
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderId = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.LeaderEpoch = val
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									val, err := protocol.ReadCompactInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								} else {
									val, err := protocol.ReadInt32Array(r)
									if err != nil {
										return err
									}
									tempElem.PreferredSuccessors = val
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem EndQuorumEpochRequestReplicaInfo
									elemR := bytes.NewReader(data)
									// CandidateId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateId = val
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadUUID(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.CandidateDirectoryId = val
									}
									consumed := len(data) - elemR.Len()
									return elem, consumed, nil
								}
								if isFlexible {
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint < 1 {
										return errors.New("invalid compact array length")
									}
									length := int32(lengthUint - 1)
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeCompactArray
									lengthBytes := protocol.EncodeVaruint32(lengthUint)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								} else {
									length, err := protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									// Collect all array elements into a buffer
									var arrayBuf bytes.Buffer
									for i := int32(0); i < length; i++ {
										// Read element into struct and encode to buffer
										var elemBuf bytes.Buffer
										elemW := &elemBuf
										var tempElem EndQuorumEpochRequestReplicaInfo
										// CandidateId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.CandidateId = val
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadUUID(r)
											if err != nil {
												return err
											}
											tempElem.CandidateDirectoryId = val
										}
										// CandidateId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.CandidateId); err != nil {
												return err
											}
										}
										// CandidateDirectoryId
										if version >= 1 && version <= 999 {
											if err := protocol.WriteUUID(elemW, tempElem.CandidateDirectoryId); err != nil {
												return err
											}
										}
										// Append to array buffer
										arrayBuf.Write(elemBuf.Bytes())
									}
									// Prepend length and decode using DecodeArray
									lengthBytes := protocol.EncodeInt32(length)
									fullData := append(lengthBytes, arrayBuf.Bytes()...)
									decoded, _, err := protocol.DecodeArray(fullData, decoder)
									if err != nil {
										return err
									}
									// Convert []interface{} to typed slice
									tempElem.PreferredCandidates = make([]EndQuorumEpochRequestReplicaInfo, len(decoded))
									for i, item := range decoded {
										tempElem.PreferredCandidates[i] = item.(EndQuorumEpochRequestReplicaInfo)
									}
								}
							}
							// PartitionIndex
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.PartitionIndex); err != nil {
									return err
								}
							}
							// LeaderId
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderId); err != nil {
									return err
								}
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.LeaderEpoch); err != nil {
									return err
								}
							}
							// PreferredSuccessors
							if version >= 0 && version <= 0 {
								if isFlexible {
									if err := protocol.WriteCompactInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32Array(elemW, tempElem.PreferredSuccessors); err != nil {
										return err
									}
								}
							}
							// PreferredCandidates
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.PreferredCandidates) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.PreferredCandidates))); err != nil {
										return err
									}
								}
								for i := range tempElem.PreferredCandidates {
									// CandidateId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.PreferredCandidates[i].CandidateId); err != nil {
											return err
										}
									}
									// CandidateDirectoryId
									if version >= 1 && version <= 999 {
										if err := protocol.WriteUUID(elemW, tempElem.PreferredCandidates[i].CandidateDirectoryId); err != nil {
											return err
										}
									}
								}
							}
							// Append to array buffer
							arrayBuf.Write(elemBuf.Bytes())
						}
						// Prepend length and decode using DecodeArray
						lengthBytes := protocol.EncodeInt32(length)
						fullData := append(lengthBytes, arrayBuf.Bytes()...)
						decoded, _, err := protocol.DecodeArray(fullData, decoder)
						if err != nil {
							return err
						}
						// Convert []interface{} to typed slice
						tempElem.Partitions = make([]EndQuorumEpochRequestPartitionData, len(decoded))
						for i, item := range decoded {
							tempElem.Partitions[i] = item.(EndQuorumEpochRequestPartitionData)
						}
					}
				}
				// TopicName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.TopicName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.TopicName); err != nil {
							return err
						}
					}
				}
				// Partitions
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Partitions) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions))); err != nil {
							return err
						}
					}
					for i := range tempElem.Partitions {
						// PartitionIndex
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PartitionIndex); err != nil {
								return err
							}
						}
						// LeaderId
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderId); err != nil {
								return err
							}
						}
						// LeaderEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].LeaderEpoch); err != nil {
								return err
							}
						}
						// PreferredSuccessors
						if version >= 0 && version <= 0 {
							if isFlexible {
								if err := protocol.WriteCompactInt32Array(elemW, tempElem.Partitions[i].PreferredSuccessors); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32Array(elemW, tempElem.Partitions[i].PreferredSuccessors); err != nil {
									return err
								}
							}
						}
						// PreferredCandidates
						if version >= 1 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Partitions[i].PreferredCandidates) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Partitions[i].PreferredCandidates))); err != nil {
									return err
								}
							}
							for i := range tempElem.Partitions[i].PreferredCandidates {
								// CandidateId
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Partitions[i].PreferredCandidates[i].CandidateId); err != nil {
										return err
									}
								}
								// CandidateDirectoryId
								if version >= 1 && version <= 999 {
									if err := protocol.WriteUUID(elemW, tempElem.Partitions[i].PreferredCandidates[i].CandidateDirectoryId); err != nil {
										return err
									}
								}
							}
						}
					}
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.Topics = make([]EndQuorumEpochRequestTopicData, len(decoded))
			for i, item := range decoded {
				m.Topics[i] = item.(EndQuorumEpochRequestTopicData)
			}
		}
	}
	// LeaderEndpoints
	if version >= 1 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem EndQuorumEpochRequestLeaderEndpoint
			elemR := bytes.NewReader(data)
			// Name
			if version >= 1 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Name = val
				}
			}
			// Host
			if version >= 1 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Host = val
				}
			}
			// Port
			if version >= 1 && version <= 999 {
			}
			// Read tagged fields if flexible
			if isFlexible {
				if err := elem.readTaggedFields(elemR, version); err != nil {
					return nil, 0, err
				}
			}
			consumed := len(data) - elemR.Len()
			return elem, consumed, nil
		}
		if isFlexible {
			lengthUint, err := protocol.ReadVaruint32(r)
			if err != nil {
				return err
			}
			if lengthUint < 1 {
				return errors.New("invalid compact array length")
			}
			length := int32(lengthUint - 1)
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem EndQuorumEpochRequestLeaderEndpoint
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 1 && version <= 999 {
				}
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 1 && version <= 999 {
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeCompactArray
			lengthBytes := protocol.EncodeVaruint32(lengthUint)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeCompactArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.LeaderEndpoints = make([]EndQuorumEpochRequestLeaderEndpoint, len(decoded))
			for i, item := range decoded {
				m.LeaderEndpoints[i] = item.(EndQuorumEpochRequestLeaderEndpoint)
			}
		} else {
			length, err := protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			// Collect all array elements into a buffer
			var arrayBuf bytes.Buffer
			for i := int32(0); i < length; i++ {
				// Read element into struct and encode to buffer
				var elemBuf bytes.Buffer
				elemW := &elemBuf
				var tempElem EndQuorumEpochRequestLeaderEndpoint
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Name = val
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.Host = val
					}
				}
				// Port
				if version >= 1 && version <= 999 {
				}
				// Name
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Name); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Name); err != nil {
							return err
						}
					}
				}
				// Host
				if version >= 1 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 1 && version <= 999 {
				}
				// Append to array buffer
				arrayBuf.Write(elemBuf.Bytes())
			}
			// Prepend length and decode using DecodeArray
			lengthBytes := protocol.EncodeInt32(length)
			fullData := append(lengthBytes, arrayBuf.Bytes()...)
			decoded, _, err := protocol.DecodeArray(fullData, decoder)
			if err != nil {
				return err
			}
			// Convert []interface{} to typed slice
			m.LeaderEndpoints = make([]EndQuorumEpochRequestLeaderEndpoint, len(decoded))
			for i, item := range decoded {
				m.LeaderEndpoints[i] = item.(EndQuorumEpochRequestLeaderEndpoint)
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

// EndQuorumEpochRequestTopicData represents The topics..
type EndQuorumEpochRequestTopicData struct {
	// The topic name.
	TopicName string `json:"topicname" versions:"0-999"`
	// The partitions.
	Partitions []EndQuorumEpochRequestPartitionData `json:"partitions" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequestTopicData.
func (m *EndQuorumEpochRequestTopicData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequestTopicData.
func (m *EndQuorumEpochRequestTopicData) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// EndQuorumEpochRequestPartitionData represents The partitions..
type EndQuorumEpochRequestPartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The current leader ID that is resigning.
	LeaderId int32 `json:"leaderid" versions:"0-999"`
	// The current epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// A sorted list of preferred successors to start the election.
	PreferredSuccessors []int32 `json:"preferredsuccessors" versions:"0"`
	// A sorted list of preferred candidates to start the election.
	PreferredCandidates []EndQuorumEpochRequestReplicaInfo `json:"preferredcandidates" versions:"1-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequestPartitionData.
func (m *EndQuorumEpochRequestPartitionData) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequestPartitionData.
func (m *EndQuorumEpochRequestPartitionData) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// EndQuorumEpochRequestReplicaInfo represents A sorted list of preferred candidates to start the election..
type EndQuorumEpochRequestReplicaInfo struct {
	// The ID of the candidate replica.
	CandidateId int32 `json:"candidateid" versions:"1-999"`
	// The directory ID of the candidate replica.
	CandidateDirectoryId uuid.UUID `json:"candidatedirectoryid" versions:"1-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequestReplicaInfo.
func (m *EndQuorumEpochRequestReplicaInfo) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequestReplicaInfo.
func (m *EndQuorumEpochRequestReplicaInfo) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// EndQuorumEpochRequestLeaderEndpoint represents Endpoints for the leader..
type EndQuorumEpochRequestLeaderEndpoint struct {
	// The name of the endpoint.
	Name string `json:"name" versions:"1-999"`
	// The node's hostname.
	Host string `json:"host" versions:"1-999"`
	// The node's port.
	Port uint16 `json:"port" versions:"1-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequestLeaderEndpoint.
func (m *EndQuorumEpochRequestLeaderEndpoint) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequestLeaderEndpoint.
func (m *EndQuorumEpochRequestLeaderEndpoint) readTaggedFields(r io.Reader, version int16) error {
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
		}
	}

	return nil
}

// writeTaggedFields writes tagged fields for EndQuorumEpochRequest.
func (m *EndQuorumEpochRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for EndQuorumEpochRequest.
func (m *EndQuorumEpochRequest) readTaggedFields(r io.Reader, version int16) error {
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
