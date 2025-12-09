package streamsgroupdescribe

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	StreamsGroupDescribeResponseApiKey        = 89
	StreamsGroupDescribeResponseHeaderVersion = 1
)

// StreamsGroupDescribeResponse represents a response message.
type StreamsGroupDescribeResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// Each described group.
	Groups []StreamsGroupDescribeResponseDescribedGroup `json:"groups" versions:"0-999"`
}

// Encode encodes a StreamsGroupDescribeResponse to a byte slice for the given version.
func (m *StreamsGroupDescribeResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a StreamsGroupDescribeResponse from a byte slice for the given version.
func (m *StreamsGroupDescribeResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a StreamsGroupDescribeResponse to an io.Writer for the given version.
func (m *StreamsGroupDescribeResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// Groups
	if version >= 0 && version <= 999 {
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(StreamsGroupDescribeResponseDescribedGroup)
			if !ok {
				return nil, errors.New("invalid type for array element")
			}
			var elemBuf bytes.Buffer
			// Temporarily use elemBuf as writer
			elemW := &elemBuf
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(elemW, structItem.ErrorCode); err != nil {
					return nil, err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteNullableString(elemW, structItem.ErrorMessage); err != nil {
						return nil, err
					}
				}
			}
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupId); err != nil {
						return nil, err
					}
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.GroupState); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.GroupState); err != nil {
						return nil, err
					}
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.GroupEpoch); err != nil {
					return nil, err
				}
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.AssignmentEpoch); err != nil {
					return nil, err
				}
			}
			// Topology
			if version >= 0 && version <= 999 {
				// Epoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, structItem.Topology.Epoch); err != nil {
						return nil, err
					}
				}
				// Subtopologies
				if version >= 0 && version <= 999 {
					if structItem.Topology.Subtopologies == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(elemW, 0); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, -1); err != nil {
								return nil, err
							}
						}
					} else {
						if isFlexible {
							length := uint32(len(structItem.Topology.Subtopologies) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Topology.Subtopologies))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Topology.Subtopologies {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].SubtopologyId); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].SubtopologyId); err != nil {
										return nil, err
									}
								}
							}
							// SourceTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, structItem.Topology.Subtopologies[i].SourceTopics); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, structItem.Topology.Subtopologies[i].SourceTopics); err != nil {
										return nil, err
									}
								}
							}
							// RepartitionSinkTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactStringArray(elemW, structItem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteStringArray(elemW, structItem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
										return nil, err
									}
								}
							}
							// StateChangelogTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(structItem.Topology.Subtopologies[i].StateChangelogTopics) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(structItem.Topology.Subtopologies[i].StateChangelogTopics))); err != nil {
										return nil, err
									}
								}
								for i := range structItem.Topology.Subtopologies[i].StateChangelogTopics {
									// Name
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
												return nil, err
											}
										} else {
											if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
												return nil, err
											}
										}
									}
									// Partitions
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].Partitions); err != nil {
											return nil, err
										}
									}
									// ReplicationFactor
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor); err != nil {
											return nil, err
										}
									}
									// TopicConfigs
									if version >= 0 && version <= 999 {
										if isFlexible {
											length := uint32(len(structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs) + 1)
											if err := protocol.WriteVaruint32(elemW, length); err != nil {
												return nil, err
											}
										} else {
											if err := protocol.WriteInt32(elemW, int32(len(structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs))); err != nil {
												return nil, err
											}
										}
										for i := range structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs {
											// Key
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
														return nil, err
													}
												} else {
													if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
														return nil, err
													}
												}
											}
											// Value
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
														return nil, err
													}
												} else {
													if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
														return nil, err
													}
												}
											}
										}
									}
								}
							}
							// RepartitionSourceTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(structItem.Topology.Subtopologies[i].RepartitionSourceTopics) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(structItem.Topology.Subtopologies[i].RepartitionSourceTopics))); err != nil {
										return nil, err
									}
								}
								for i := range structItem.Topology.Subtopologies[i].RepartitionSourceTopics {
									// Name
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
												return nil, err
											}
										} else {
											if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
												return nil, err
											}
										}
									}
									// Partitions
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions); err != nil {
											return nil, err
										}
									}
									// ReplicationFactor
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor); err != nil {
											return nil, err
										}
									}
									// TopicConfigs
									if version >= 0 && version <= 999 {
										if isFlexible {
											length := uint32(len(structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs) + 1)
											if err := protocol.WriteVaruint32(elemW, length); err != nil {
												return nil, err
											}
										} else {
											if err := protocol.WriteInt32(elemW, int32(len(structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs))); err != nil {
												return nil, err
											}
										}
										for i := range structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs {
											// Key
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
														return nil, err
													}
												} else {
													if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
														return nil, err
													}
												}
											}
											// Value
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
														return nil, err
													}
												} else {
													if err := protocol.WriteString(elemW, structItem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
														return nil, err
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
			// Members
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Members) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Members))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Members {
					// MemberId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].MemberId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].MemberId); err != nil {
								return nil, err
							}
						}
					}
					// MemberEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Members[i].MemberEpoch); err != nil {
							return nil, err
						}
					}
					// InstanceId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Members[i].InstanceId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Members[i].InstanceId); err != nil {
								return nil, err
							}
						}
					}
					// RackId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Members[i].RackId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Members[i].RackId); err != nil {
								return nil, err
							}
						}
					}
					// ClientId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].ClientId); err != nil {
								return nil, err
							}
						}
					}
					// ClientHost
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientHost); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].ClientHost); err != nil {
								return nil, err
							}
						}
					}
					// TopologyEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, structItem.Members[i].TopologyEpoch); err != nil {
							return nil, err
						}
					}
					// ProcessId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Members[i].ProcessId); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Members[i].ProcessId); err != nil {
								return nil, err
							}
						}
					}
					// UserEndpoint
					if version >= 0 && version <= 999 {
					}
					// ClientTags
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Members[i].ClientTags) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Members[i].ClientTags))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Members[i].ClientTags {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientTags[i].Key); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Members[i].ClientTags[i].Key); err != nil {
										return nil, err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Members[i].ClientTags[i].Value); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Members[i].ClientTags[i].Value); err != nil {
										return nil, err
									}
								}
							}
						}
					}
					// TaskOffsets
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Members[i].TaskOffsets) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Members[i].TaskOffsets))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Members[i].TaskOffsets {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
										return nil, err
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Members[i].TaskOffsets[i].Partition); err != nil {
									return nil, err
								}
							}
							// Offset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Members[i].TaskOffsets[i].Offset); err != nil {
									return nil, err
								}
							}
						}
					}
					// TaskEndOffsets
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Members[i].TaskEndOffsets) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Members[i].TaskEndOffsets))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Members[i].TaskEndOffsets {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
										return nil, err
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, structItem.Members[i].TaskEndOffsets[i].Partition); err != nil {
									return nil, err
								}
							}
							// Offset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(elemW, structItem.Members[i].TaskEndOffsets[i].Offset); err != nil {
									return nil, err
								}
							}
						}
					}
					// Assignment
					if version >= 0 && version <= 999 {
					}
					// TargetAssignment
					if version >= 0 && version <= 999 {
					}
					// IsClassic
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(elemW, structItem.Members[i].IsClassic); err != nil {
							return nil, err
						}
					}
				}
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(elemW, structItem.AuthorizedOperations); err != nil {
					return nil, err
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
		items := make([]interface{}, len(m.Groups))
		for i := range m.Groups {
			items[i] = m.Groups[i]
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

// Read reads a StreamsGroupDescribeResponse from an io.Reader for the given version.
func (m *StreamsGroupDescribeResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
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
	// Groups
	if version >= 0 && version <= 999 {
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem StreamsGroupDescribeResponseDescribedGroup
			elemR := bytes.NewReader(data)
			// ErrorCode
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt16(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ErrorCode = val
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				} else {
					val, err := protocol.ReadNullableString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ErrorMessage = val
				}
			}
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupId = val
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupState = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.GroupState = val
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.GroupEpoch = val
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.AssignmentEpoch = val
			}
			// Topology
			if version >= 0 && version <= 999 {
				// Epoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.Topology.Epoch = val
				}
				// Subtopologies
				if version >= 0 && version <= 999 {
					// Nested array in decoder - manual handling needed
					return nil, 0, errors.New("nested arrays in decoder not fully supported")
				}
			}
			// Members
			if version >= 0 && version <= 999 {
				// Nested array in decoder - manual handling needed
				return nil, 0, errors.New("nested arrays in decoder not fully supported")
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt32(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.AuthorizedOperations = val
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
				var tempElem StreamsGroupDescribeResponseDescribedGroup
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AssignmentEpoch = val
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						val, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						tempElem.Topology.Epoch = val
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						// Decode array using ArrayDecoder
						decoder := func(data []byte) (interface{}, int, error) {
							var elem StreamsGroupDescribeResponseSubtopology
							elemR := bytes.NewReader(data)
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SubtopologyId = val
								} else {
									val, err := protocol.ReadString(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SubtopologyId = val
								}
							}
							// SourceTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SourceTopics = val
								} else {
									val, err := protocol.ReadStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SourceTopics = val
								}
							}
							// RepartitionSinkTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.RepartitionSinkTopics = val
								} else {
									val, err := protocol.ReadStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.RepartitionSinkTopics = val
								}
							}
							// StateChangelogTopics
							if version >= 0 && version <= 999 {
								// Nested array in decoder - manual handling needed
								return nil, 0, errors.New("nested arrays in decoder not fully supported")
							}
							// RepartitionSourceTopics
							if version >= 0 && version <= 999 {
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
							if lengthUint == 0 {
								tempElem.Topology.Subtopologies = nil
								return nil
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
								var tempElem StreamsGroupDescribeResponseSubtopology
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
										}
									}
								}
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
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
							tempElem.Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, len(decoded))
							for i, item := range decoded {
								tempElem.Topology.Subtopologies[i] = item.(StreamsGroupDescribeResponseSubtopology)
							}
						} else {
							length, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							if length == -1 {
								tempElem.Topology.Subtopologies = nil
								return nil
							}
							// Collect all array elements into a buffer
							var arrayBuf bytes.Buffer
							for i := int32(0); i < length; i++ {
								// Read element into struct and encode to buffer
								var elemBuf bytes.Buffer
								elemW := &elemBuf
								var tempElem StreamsGroupDescribeResponseSubtopology
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
										}
									}
								}
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
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
							tempElem.Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, len(decoded))
							for i, item := range decoded {
								tempElem.Topology.Subtopologies[i] = item.(StreamsGroupDescribeResponseSubtopology)
							}
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem StreamsGroupDescribeResponseMember
						elemR := bytes.NewReader(data)
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.MemberEpoch = val
						}
						// InstanceId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.InstanceId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.InstanceId = val
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							}
						}
						// TopologyEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopologyEpoch = val
						}
						// ProcessId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ProcessId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ProcessId = val
							}
						}
						// UserEndpoint
						if version >= 0 && version <= 999 {
						}
						// ClientTags
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// TaskOffsets
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// TaskEndOffsets
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
						// TargetAssignment
						if version >= 0 && version <= 999 {
						}
						// IsClassic
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsClassic = val
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
							var tempElem StreamsGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem KeyValue
									elemR := bytes.NewReader(data)
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsClassic = val
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.TopologyEpoch); err != nil {
									return err
								}
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ClientTags) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ClientTags))); err != nil {
										return err
									}
								}
								for i := range tempElem.ClientTags {
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										}
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskEndOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskEndOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskEndOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskEndOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskEndOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsClassic); err != nil {
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
						tempElem.Members = make([]StreamsGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(StreamsGroupDescribeResponseMember)
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
							var tempElem StreamsGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem KeyValue
									elemR := bytes.NewReader(data)
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsClassic = val
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.TopologyEpoch); err != nil {
									return err
								}
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ClientTags) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ClientTags))); err != nil {
										return err
									}
								}
								for i := range tempElem.ClientTags {
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										}
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskEndOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskEndOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskEndOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskEndOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskEndOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsClassic); err != nil {
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
						tempElem.Members = make([]StreamsGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(StreamsGroupDescribeResponseMember)
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.GroupEpoch); err != nil {
						return err
					}
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AssignmentEpoch); err != nil {
						return err
					}
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, tempElem.Topology.Epoch); err != nil {
							return err
						}
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						if tempElem.Topology.Subtopologies == nil {
							if isFlexible {
								if err := protocol.WriteVaruint32(elemW, 0); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, -1); err != nil {
									return err
								}
							}
						} else {
							if isFlexible {
								length := uint32(len(tempElem.Topology.Subtopologies) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topology.Subtopologies {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.Topology.Subtopologies[i].SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.Topology.Subtopologies[i].SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.Topology.Subtopologies[i].StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.Topology.Subtopologies[i].RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Members) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members))); err != nil {
							return err
						}
					}
					for i := range tempElem.Members {
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].MemberEpoch); err != nil {
								return err
							}
						}
						// InstanceId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].InstanceId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].InstanceId); err != nil {
									return err
								}
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							}
						}
						// TopologyEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].TopologyEpoch); err != nil {
								return err
							}
						}
						// ProcessId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ProcessId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ProcessId); err != nil {
									return err
								}
							}
						}
						// UserEndpoint
						if version >= 0 && version <= 999 {
						}
						// ClientTags
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].ClientTags) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].ClientTags))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].ClientTags {
								// Key
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientTags[i].Key); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].ClientTags[i].Key); err != nil {
											return err
										}
									}
								}
								// Value
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientTags[i].Value); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].ClientTags[i].Value); err != nil {
											return err
										}
									}
								}
							}
						}
						// TaskOffsets
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].TaskOffsets) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].TaskOffsets))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].TaskOffsets {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// Partition
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Members[i].TaskOffsets[i].Partition); err != nil {
										return err
									}
								}
								// Offset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Members[i].TaskOffsets[i].Offset); err != nil {
										return err
									}
								}
							}
						}
						// TaskEndOffsets
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].TaskEndOffsets) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].TaskEndOffsets))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].TaskEndOffsets {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// Partition
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Members[i].TaskEndOffsets[i].Partition); err != nil {
										return err
									}
								}
								// Offset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Members[i].TaskEndOffsets[i].Offset); err != nil {
										return err
									}
								}
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
						// TargetAssignment
						if version >= 0 && version <= 999 {
						}
						// IsClassic
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Members[i].IsClassic); err != nil {
								return err
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AuthorizedOperations); err != nil {
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
			m.Groups = make([]StreamsGroupDescribeResponseDescribedGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(StreamsGroupDescribeResponseDescribedGroup)
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
				var tempElem StreamsGroupDescribeResponseDescribedGroup
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					tempElem.ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						tempElem.ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AssignmentEpoch = val
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						val, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						tempElem.Topology.Epoch = val
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						// Decode array using ArrayDecoder
						decoder := func(data []byte) (interface{}, int, error) {
							var elem StreamsGroupDescribeResponseSubtopology
							elemR := bytes.NewReader(data)
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SubtopologyId = val
								} else {
									val, err := protocol.ReadString(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SubtopologyId = val
								}
							}
							// SourceTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SourceTopics = val
								} else {
									val, err := protocol.ReadStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.SourceTopics = val
								}
							}
							// RepartitionSinkTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.RepartitionSinkTopics = val
								} else {
									val, err := protocol.ReadStringArray(elemR)
									if err != nil {
										return nil, 0, err
									}
									elem.RepartitionSinkTopics = val
								}
							}
							// StateChangelogTopics
							if version >= 0 && version <= 999 {
								// Nested array in decoder - manual handling needed
								return nil, 0, errors.New("nested arrays in decoder not fully supported")
							}
							// RepartitionSourceTopics
							if version >= 0 && version <= 999 {
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
							if lengthUint == 0 {
								tempElem.Topology.Subtopologies = nil
								return nil
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
								var tempElem StreamsGroupDescribeResponseSubtopology
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
										}
									}
								}
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
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
							tempElem.Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, len(decoded))
							for i, item := range decoded {
								tempElem.Topology.Subtopologies[i] = item.(StreamsGroupDescribeResponseSubtopology)
							}
						} else {
							length, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							if length == -1 {
								tempElem.Topology.Subtopologies = nil
								return nil
							}
							// Collect all array elements into a buffer
							var arrayBuf bytes.Buffer
							for i := int32(0); i < length; i++ {
								// Read element into struct and encode to buffer
								var elemBuf bytes.Buffer
								elemW := &elemBuf
								var tempElem StreamsGroupDescribeResponseSubtopology
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									} else {
										val, err := protocol.ReadString(r)
										if err != nil {
											return err
										}
										tempElem.SubtopologyId = val
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.SourceTopics = val
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										val, err := protocol.ReadCompactStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									} else {
										val, err := protocol.ReadStringArray(r)
										if err != nil {
											return err
										}
										tempElem.RepartitionSinkTopics = val
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.StateChangelogTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.StateChangelogTopics[i] = item.(TopicInfo)
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									// Decode array using ArrayDecoder
									decoder := func(data []byte) (interface{}, int, error) {
										var elem TopicInfo
										elemR := bytes.NewReader(data)
										// Name
										if version >= 0 && version <= 999 {
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
										// Partitions
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Partitions = val
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.ReplicationFactor = val
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
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
											var tempElem TopicInfo
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												tempElem.Partitions = val
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												val, err := protocol.ReadInt16(r)
												if err != nil {
													return err
												}
												tempElem.ReplicationFactor = val
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												// Decode array using ArrayDecoder
												decoder := func(data []byte) (interface{}, int, error) {
													var elem KeyValue
													elemR := bytes.NewReader(data)
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Key = val
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															val, err := protocol.ReadCompactString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
														} else {
															val, err := protocol.ReadString(elemR)
															if err != nil {
																return nil, 0, err
															}
															elem.Value = val
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
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
														var tempElem KeyValue
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Key = val
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																val, err := protocol.ReadCompactString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															} else {
																val, err := protocol.ReadString(r)
																if err != nil {
																	return err
																}
																tempElem.Value = val
															}
														}
														// Key
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
																	return err
																}
															}
														}
														// Value
														if version >= 0 && version <= 999 {
															if isFlexible {
																if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
																	return err
																}
															} else {
																if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
																	return err
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
													tempElem.TopicConfigs = make([]KeyValue, len(decoded))
													for i, item := range decoded {
														tempElem.TopicConfigs[i] = item.(KeyValue)
													}
												}
											}
											// Name
											if version >= 0 && version <= 999 {
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
											// Partitions
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt32(elemW, tempElem.Partitions); err != nil {
													return err
												}
											}
											// ReplicationFactor
											if version >= 0 && version <= 999 {
												if err := protocol.WriteInt16(elemW, tempElem.ReplicationFactor); err != nil {
													return err
												}
											}
											// TopicConfigs
											if version >= 0 && version <= 999 {
												if isFlexible {
													length := uint32(len(tempElem.TopicConfigs) + 1)
													if err := protocol.WriteVaruint32(elemW, length); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteInt32(elemW, int32(len(tempElem.TopicConfigs))); err != nil {
														return err
													}
												}
												for i := range tempElem.TopicConfigs {
													// Key
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Key); err != nil {
																return err
															}
														}
													}
													// Value
													if version >= 0 && version <= 999 {
														if isFlexible {
															if err := protocol.WriteCompactString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
															}
														} else {
															if err := protocol.WriteString(elemW, tempElem.TopicConfigs[i].Value); err != nil {
																return err
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
										tempElem.RepartitionSourceTopics = make([]TopicInfo, len(decoded))
										for i, item := range decoded {
											tempElem.RepartitionSourceTopics[i] = item.(TopicInfo)
										}
									}
								}
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
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
							tempElem.Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, len(decoded))
							for i, item := range decoded {
								tempElem.Topology.Subtopologies[i] = item.(StreamsGroupDescribeResponseSubtopology)
							}
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem StreamsGroupDescribeResponseMember
						elemR := bytes.NewReader(data)
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.MemberId = val
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.MemberEpoch = val
						}
						// InstanceId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.InstanceId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.InstanceId = val
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.RackId = val
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientId = val
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ClientHost = val
							}
						}
						// TopologyEpoch
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadInt32(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.TopologyEpoch = val
						}
						// ProcessId
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ProcessId = val
							} else {
								val, err := protocol.ReadString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.ProcessId = val
							}
						}
						// UserEndpoint
						if version >= 0 && version <= 999 {
						}
						// ClientTags
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// TaskOffsets
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// TaskEndOffsets
						if version >= 0 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
						// TargetAssignment
						if version >= 0 && version <= 999 {
						}
						// IsClassic
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsClassic = val
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
							var tempElem StreamsGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem KeyValue
									elemR := bytes.NewReader(data)
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsClassic = val
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.TopologyEpoch); err != nil {
									return err
								}
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ClientTags) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ClientTags))); err != nil {
										return err
									}
								}
								for i := range tempElem.ClientTags {
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										}
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskEndOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskEndOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskEndOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskEndOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskEndOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsClassic); err != nil {
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
						tempElem.Members = make([]StreamsGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(StreamsGroupDescribeResponseMember)
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
							var tempElem StreamsGroupDescribeResponseMember
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								tempElem.TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									tempElem.ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem KeyValue
									elemR := bytes.NewReader(data)
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Key = val
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
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
										var tempElem KeyValue
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Key); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Key); err != nil {
													return err
												}
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Value); err != nil {
													return err
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
									tempElem.ClientTags = make([]KeyValue, len(decoded))
									for i, item := range decoded {
										tempElem.ClientTags[i] = item.(KeyValue)
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem TaskOffset
									elemR := bytes.NewReader(data)
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										} else {
											val, err := protocol.ReadString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.SubtopologyId = val
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt32(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Partition = val
									}
									// Offset
									if version >= 0 && version <= 999 {
										val, err := protocol.ReadInt64(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Offset = val
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
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
										var tempElem TaskOffset
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												tempElem.SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											tempElem.Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											tempElem.Offset = val
										}
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.SubtopologyId); err != nil {
													return err
												}
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Partition); err != nil {
												return err
											}
										}
										// Offset
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt64(elemW, tempElem.Offset); err != nil {
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
									tempElem.TaskEndOffsets = make([]TaskOffset, len(decoded))
									for i, item := range decoded {
										tempElem.TaskEndOffsets[i] = item.(TaskOffset)
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsClassic = val
							}
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.MemberId); err != nil {
										return err
									}
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.MemberEpoch); err != nil {
									return err
								}
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.InstanceId); err != nil {
										return err
									}
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.RackId); err != nil {
										return err
									}
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientId); err != nil {
										return err
									}
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ClientHost); err != nil {
										return err
									}
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(elemW, tempElem.TopologyEpoch); err != nil {
									return err
								}
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(elemW, tempElem.ProcessId); err != nil {
										return err
									}
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.ClientTags) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.ClientTags))); err != nil {
										return err
									}
								}
								for i := range tempElem.ClientTags {
									// Key
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Key); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.ClientTags[i].Value); err != nil {
												return err
											}
										}
									}
								}
							}
							// TaskOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// TaskEndOffsets
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.TaskEndOffsets) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.TaskEndOffsets))); err != nil {
										return err
									}
								}
								for i := range tempElem.TaskEndOffsets {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.TaskEndOffsets[i].SubtopologyId); err != nil {
												return err
											}
										}
									}
									// Partition
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(elemW, tempElem.TaskEndOffsets[i].Partition); err != nil {
											return err
										}
									}
									// Offset
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt64(elemW, tempElem.TaskEndOffsets[i].Offset); err != nil {
											return err
										}
									}
								}
							}
							// Assignment
							if version >= 0 && version <= 999 {
							}
							// TargetAssignment
							if version >= 0 && version <= 999 {
							}
							// IsClassic
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsClassic); err != nil {
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
						tempElem.Members = make([]StreamsGroupDescribeResponseMember, len(decoded))
						for i, item := range decoded {
							tempElem.Members[i] = item.(StreamsGroupDescribeResponseMember)
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					tempElem.AuthorizedOperations = val
				}
				// ErrorCode
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt16(elemW, tempElem.ErrorCode); err != nil {
						return err
					}
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(elemW, tempElem.ErrorMessage); err != nil {
							return err
						}
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupId); err != nil {
							return err
						}
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.GroupState); err != nil {
							return err
						}
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.GroupEpoch); err != nil {
						return err
					}
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AssignmentEpoch); err != nil {
						return err
					}
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(elemW, tempElem.Topology.Epoch); err != nil {
							return err
						}
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						if tempElem.Topology.Subtopologies == nil {
							if isFlexible {
								if err := protocol.WriteVaruint32(elemW, 0); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, -1); err != nil {
									return err
								}
							}
						} else {
							if isFlexible {
								length := uint32(len(tempElem.Topology.Subtopologies) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies))); err != nil {
									return err
								}
							}
							for i := range tempElem.Topology.Subtopologies {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// SourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.Topology.Subtopologies[i].SourceTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.Topology.Subtopologies[i].SourceTopics); err != nil {
											return err
										}
									}
								}
								// RepartitionSinkTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactStringArray(elemW, tempElem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteStringArray(elemW, tempElem.Topology.Subtopologies[i].RepartitionSinkTopics); err != nil {
											return err
										}
									}
								}
								// StateChangelogTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.Topology.Subtopologies[i].StateChangelogTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
								// RepartitionSourceTopics
								if version >= 0 && version <= 999 {
									if isFlexible {
										length := uint32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics) + 1)
										if err := protocol.WriteVaruint32(elemW, length); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics))); err != nil {
											return err
										}
									}
									for i := range tempElem.Topology.Subtopologies[i].RepartitionSourceTopics {
										// Name
										if version >= 0 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
													return err
												}
											}
										}
										// Partitions
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt32(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions); err != nil {
												return err
											}
										}
										// ReplicationFactor
										if version >= 0 && version <= 999 {
											if err := protocol.WriteInt16(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor); err != nil {
												return err
											}
										}
										// TopicConfigs
										if version >= 0 && version <= 999 {
											if isFlexible {
												length := uint32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs) + 1)
												if err := protocol.WriteVaruint32(elemW, length); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteInt32(elemW, int32(len(tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs))); err != nil {
													return err
												}
											}
											for i := range tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs {
												// Key
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
															return err
														}
													}
												}
												// Value
												if version >= 0 && version <= 999 {
													if isFlexible {
														if err := protocol.WriteCompactString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													} else {
														if err := protocol.WriteString(elemW, tempElem.Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
															return err
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
				// Members
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Members) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members))); err != nil {
							return err
						}
					}
					for i := range tempElem.Members {
						// MemberId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].MemberId); err != nil {
									return err
								}
							}
						}
						// MemberEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].MemberEpoch); err != nil {
								return err
							}
						}
						// InstanceId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].InstanceId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].InstanceId); err != nil {
									return err
								}
							}
						}
						// RackId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Members[i].RackId); err != nil {
									return err
								}
							}
						}
						// ClientId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientId); err != nil {
									return err
								}
							}
						}
						// ClientHost
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ClientHost); err != nil {
									return err
								}
							}
						}
						// TopologyEpoch
						if version >= 0 && version <= 999 {
							if err := protocol.WriteInt32(elemW, tempElem.Members[i].TopologyEpoch); err != nil {
								return err
							}
						}
						// ProcessId
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ProcessId); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Members[i].ProcessId); err != nil {
									return err
								}
							}
						}
						// UserEndpoint
						if version >= 0 && version <= 999 {
						}
						// ClientTags
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].ClientTags) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].ClientTags))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].ClientTags {
								// Key
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientTags[i].Key); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].ClientTags[i].Key); err != nil {
											return err
										}
									}
								}
								// Value
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].ClientTags[i].Value); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].ClientTags[i].Value); err != nil {
											return err
										}
									}
								}
							}
						}
						// TaskOffsets
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].TaskOffsets) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].TaskOffsets))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].TaskOffsets {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].TaskOffsets[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// Partition
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Members[i].TaskOffsets[i].Partition); err != nil {
										return err
									}
								}
								// Offset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Members[i].TaskOffsets[i].Offset); err != nil {
										return err
									}
								}
							}
						}
						// TaskEndOffsets
						if version >= 0 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Members[i].TaskEndOffsets) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Members[i].TaskEndOffsets))); err != nil {
									return err
								}
							}
							for i := range tempElem.Members[i].TaskEndOffsets {
								// SubtopologyId
								if version >= 0 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
											return err
										}
									}
								}
								// Partition
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt32(elemW, tempElem.Members[i].TaskEndOffsets[i].Partition); err != nil {
										return err
									}
								}
								// Offset
								if version >= 0 && version <= 999 {
									if err := protocol.WriteInt64(elemW, tempElem.Members[i].TaskEndOffsets[i].Offset); err != nil {
										return err
									}
								}
							}
						}
						// Assignment
						if version >= 0 && version <= 999 {
						}
						// TargetAssignment
						if version >= 0 && version <= 999 {
						}
						// IsClassic
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Members[i].IsClassic); err != nil {
								return err
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(elemW, tempElem.AuthorizedOperations); err != nil {
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
			m.Groups = make([]StreamsGroupDescribeResponseDescribedGroup, len(decoded))
			for i, item := range decoded {
				m.Groups[i] = item.(StreamsGroupDescribeResponseDescribedGroup)
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

// StreamsGroupDescribeResponseDescribedGroup represents Each described group..
type StreamsGroupDescribeResponseDescribedGroup struct {
	// The describe error, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The top-level error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The group ID string.
	GroupId string `json:"groupid" versions:"0-999"`
	// The group state string, or the empty string.
	GroupState string `json:"groupstate" versions:"0-999"`
	// The group epoch.
	GroupEpoch int32 `json:"groupepoch" versions:"0-999"`
	// The assignment epoch.
	AssignmentEpoch int32 `json:"assignmentepoch" versions:"0-999"`
	// The topology metadata currently initialized for the streams application. Can be null in case of a describe error.
	Topology StreamsGroupDescribeResponseTopology `json:"topology" versions:"0-999"`
	// The members.
	Members []StreamsGroupDescribeResponseMember `json:"members" versions:"0-999"`
	// 32-bit bitfield to represent authorized operations for this group.
	AuthorizedOperations int32 `json:"authorizedoperations" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for StreamsGroupDescribeResponseDescribedGroup.
func (m *StreamsGroupDescribeResponseDescribedGroup) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for StreamsGroupDescribeResponseDescribedGroup.
func (m *StreamsGroupDescribeResponseDescribedGroup) readTaggedFields(r io.Reader, version int16) error {
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

// StreamsGroupDescribeResponseTopology represents The topology metadata currently initialized for the streams application. Can be null in case of a describe error..
type StreamsGroupDescribeResponseTopology struct {
	// The epoch of the currently initialized topology for this group.
	Epoch int32 `json:"epoch" versions:"0-999"`
	// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
	Subtopologies []StreamsGroupDescribeResponseSubtopology `json:"subtopologies" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for StreamsGroupDescribeResponseTopology.
func (m *StreamsGroupDescribeResponseTopology) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for StreamsGroupDescribeResponseTopology.
func (m *StreamsGroupDescribeResponseTopology) readTaggedFields(r io.Reader, version int16) error {
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

// StreamsGroupDescribeResponseSubtopology represents The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned..
type StreamsGroupDescribeResponseSubtopology struct {
	// String to uniquely identify the subtopology.
	SubtopologyId string `json:"subtopologyid" versions:"0-999"`
	// The topics the subtopology reads from.
	SourceTopics []string `json:"sourcetopics" versions:"0-999"`
	// The repartition topics the subtopology writes to.
	RepartitionSinkTopics []string `json:"repartitionsinktopics" versions:"0-999"`
	// The set of state changelog topics associated with this subtopology. Created automatically.
	StateChangelogTopics []TopicInfo `json:"statechangelogtopics" versions:"0-999"`
	// The set of source topics that are internally created repartition topics. Created automatically.
	RepartitionSourceTopics []TopicInfo `json:"repartitionsourcetopics" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for StreamsGroupDescribeResponseSubtopology.
func (m *StreamsGroupDescribeResponseSubtopology) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for StreamsGroupDescribeResponseSubtopology.
func (m *StreamsGroupDescribeResponseSubtopology) readTaggedFields(r io.Reader, version int16) error {
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

// StreamsGroupDescribeResponseMember represents The members..
type StreamsGroupDescribeResponseMember struct {
	// The member ID.
	MemberId string `json:"memberid" versions:"0-999"`
	// The member epoch.
	MemberEpoch int32 `json:"memberepoch" versions:"0-999"`
	// The member instance ID for static membership.
	InstanceId *string `json:"instanceid" versions:"0-999"`
	// The rack ID.
	RackId *string `json:"rackid" versions:"0-999"`
	// The client ID.
	ClientId string `json:"clientid" versions:"0-999"`
	// The client host.
	ClientHost string `json:"clienthost" versions:"0-999"`
	// The epoch of the topology on the client.
	TopologyEpoch int32 `json:"topologyepoch" versions:"0-999"`
	// Identity of the streams instance that may have multiple clients.
	ProcessId string `json:"processid" versions:"0-999"`
	// User-defined endpoint for Interactive Queries. Null if not defined for this client.
	UserEndpoint Endpoint `json:"userendpoint" versions:"0-999"`
	// Used for rack-aware assignment algorithm.
	ClientTags []KeyValue `json:"clienttags" versions:"0-999"`
	// Cumulative changelog offsets for tasks.
	TaskOffsets []TaskOffset `json:"taskoffsets" versions:"0-999"`
	// Cumulative changelog end offsets for tasks.
	TaskEndOffsets []TaskOffset `json:"taskendoffsets" versions:"0-999"`
	// The current assignment.
	Assignment Assignment `json:"assignment" versions:"0-999"`
	// The target assignment.
	TargetAssignment Assignment `json:"targetassignment" versions:"0-999"`
	// True for classic members that have not been upgraded yet.
	IsClassic bool `json:"isclassic" versions:"0-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for StreamsGroupDescribeResponseMember.
func (m *StreamsGroupDescribeResponseMember) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for StreamsGroupDescribeResponseMember.
func (m *StreamsGroupDescribeResponseMember) readTaggedFields(r io.Reader, version int16) error {
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

// writeTaggedFields writes tagged fields for StreamsGroupDescribeResponse.
func (m *StreamsGroupDescribeResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for StreamsGroupDescribeResponse.
func (m *StreamsGroupDescribeResponse) readTaggedFields(r io.Reader, version int16) error {
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
