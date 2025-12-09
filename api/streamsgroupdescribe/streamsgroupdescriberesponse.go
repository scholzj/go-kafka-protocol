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
		if isFlexible {
			length := uint32(len(m.Groups) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Groups))); err != nil {
				return err
			}
		}
		for i := range m.Groups {
			// ErrorCode
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt16(w, m.Groups[i].ErrorCode); err != nil {
					return err
				}
			}
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Groups[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Groups[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// GroupId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].GroupId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].GroupId); err != nil {
						return err
					}
				}
			}
			// GroupState
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Groups[i].GroupState); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Groups[i].GroupState); err != nil {
						return err
					}
				}
			}
			// GroupEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].GroupEpoch); err != nil {
					return err
				}
			}
			// AssignmentEpoch
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].AssignmentEpoch); err != nil {
					return err
				}
			}
			// Topology
			if version >= 0 && version <= 999 {
				// Epoch
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt32(w, m.Groups[i].Topology.Epoch); err != nil {
						return err
					}
				}
				// Subtopologies
				if version >= 0 && version <= 999 {
					if m.Groups[i].Topology.Subtopologies == nil {
						if isFlexible {
							if err := protocol.WriteVaruint32(w, 0); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, -1); err != nil {
								return err
							}
						}
					} else {
						if isFlexible {
							length := uint32(len(m.Groups[i].Topology.Subtopologies) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies))); err != nil {
								return err
							}
						}
						for i := range m.Groups[i].Topology.Subtopologies {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].SubtopologyId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].SubtopologyId); err != nil {
										return err
									}
								}
							}
							// SourceTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(m.Groups[i].Topology.Subtopologies[i].SourceTopics) + 1)
									if err := protocol.WriteVaruint32(w, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].SourceTopics))); err != nil {
										return err
									}
								}
								for i := range m.Groups[i].Topology.Subtopologies[i].SourceTopics {
									if isFlexible {
										if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].SourceTopics[i]); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].SourceTopics[i]); err != nil {
											return err
										}
									}
									_ = i
								}
							}
							// RepartitionSinkTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics) + 1)
									if err := protocol.WriteVaruint32(w, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics))); err != nil {
										return err
									}
								}
								for i := range m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics {
									if isFlexible {
										if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i]); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i]); err != nil {
											return err
										}
									}
									_ = i
								}
							}
							// StateChangelogTopics
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics) + 1)
									if err := protocol.WriteVaruint32(w, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics))); err != nil {
										return err
									}
								}
								for i := range m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics {
									// Name
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name); err != nil {
												return err
											}
										}
									}
									// Partitions
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions); err != nil {
											return err
										}
									}
									// ReplicationFactor
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor); err != nil {
											return err
										}
									}
									// TopicConfigs
									if version >= 0 && version <= 999 {
										if isFlexible {
											length := uint32(len(m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs) + 1)
											if err := protocol.WriteVaruint32(w, length); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs))); err != nil {
												return err
											}
										}
										for i := range m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs {
											// Key
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key); err != nil {
														return err
													}
												}
											}
											// Value
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value); err != nil {
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
									length := uint32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics) + 1)
									if err := protocol.WriteVaruint32(w, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics))); err != nil {
										return err
									}
								}
								for i := range m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics {
									// Name
									if version >= 0 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name); err != nil {
												return err
											}
										}
									}
									// Partitions
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt32(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions); err != nil {
											return err
										}
									}
									// ReplicationFactor
									if version >= 0 && version <= 999 {
										if err := protocol.WriteInt16(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor); err != nil {
											return err
										}
									}
									// TopicConfigs
									if version >= 0 && version <= 999 {
										if isFlexible {
											length := uint32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs) + 1)
											if err := protocol.WriteVaruint32(w, length); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs))); err != nil {
												return err
											}
										}
										for i := range m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs {
											// Key
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key); err != nil {
														return err
													}
												}
											}
											// Value
											if version >= 0 && version <= 999 {
												if isFlexible {
													if err := protocol.WriteCompactString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
														return err
													}
												} else {
													if err := protocol.WriteString(w, m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value); err != nil {
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
					length := uint32(len(m.Groups[i].Members) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members))); err != nil {
						return err
					}
				}
				for i := range m.Groups[i].Members {
					// MemberId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].MemberId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].MemberId); err != nil {
								return err
							}
						}
					}
					// MemberEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Groups[i].Members[i].MemberEpoch); err != nil {
							return err
						}
					}
					// InstanceId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].InstanceId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].InstanceId); err != nil {
								return err
							}
						}
					}
					// RackId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Groups[i].Members[i].RackId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Groups[i].Members[i].RackId); err != nil {
								return err
							}
						}
					}
					// ClientId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientId); err != nil {
								return err
							}
						}
					}
					// ClientHost
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientHost); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientHost); err != nil {
								return err
							}
						}
					}
					// TopologyEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Groups[i].Members[i].TopologyEpoch); err != nil {
							return err
						}
					}
					// ProcessId
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ProcessId); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Groups[i].Members[i].ProcessId); err != nil {
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
							length := uint32(len(m.Groups[i].Members[i].ClientTags) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members[i].ClientTags))); err != nil {
								return err
							}
						}
						for i := range m.Groups[i].Members[i].ClientTags {
							// Key
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientTags[i].Key); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientTags[i].Key); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].ClientTags[i].Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Groups[i].Members[i].ClientTags[i].Value); err != nil {
										return err
									}
								}
							}
						}
					}
					// TaskOffsets
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Groups[i].Members[i].TaskOffsets) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members[i].TaskOffsets))); err != nil {
								return err
							}
						}
						for i := range m.Groups[i].Members[i].TaskOffsets {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId); err != nil {
										return err
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Groups[i].Members[i].TaskOffsets[i].Partition); err != nil {
									return err
								}
							}
							// Offset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Groups[i].Members[i].TaskOffsets[i].Offset); err != nil {
									return err
								}
							}
						}
					}
					// TaskEndOffsets
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Groups[i].Members[i].TaskEndOffsets) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Groups[i].Members[i].TaskEndOffsets))); err != nil {
								return err
							}
						}
						for i := range m.Groups[i].Members[i].TaskEndOffsets {
							// SubtopologyId
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId); err != nil {
										return err
									}
								}
							}
							// Partition
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Groups[i].Members[i].TaskEndOffsets[i].Partition); err != nil {
									return err
								}
							}
							// Offset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Groups[i].Members[i].TaskEndOffsets[i].Offset); err != nil {
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
						if err := protocol.WriteBool(w, m.Groups[i].Members[i].IsClassic); err != nil {
							return err
						}
					}
				}
			}
			// AuthorizedOperations
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt32(w, m.Groups[i].AuthorizedOperations); err != nil {
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
			m.Groups = make([]StreamsGroupDescribeResponseDescribedGroup, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Groups[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AssignmentEpoch = val
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						val, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Topology.Epoch = val
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						var length int32
						if isFlexible {
							var lengthUint uint32
							lengthUint, err := protocol.ReadVaruint32(r)
							if err != nil {
								return err
							}
							if lengthUint == 0 {
								m.Groups[i].Topology.Subtopologies = nil
							} else {
								if lengthUint < 1 {
									return errors.New("invalid compact array length")
								}
								length = int32(lengthUint - 1)
								m.Groups[i].Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, length)
								for i := int32(0); i < length; i++ {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										}
									}
									// SourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										}
									}
									// RepartitionSinkTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										}
									}
									// StateChangelogTopics
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													}
												}
											}
										}
									}
									// RepartitionSourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							if length == -1 {
								m.Groups[i].Topology.Subtopologies = nil
							} else {
								m.Groups[i].Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, length)
								for i := int32(0); i < length; i++ {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										}
									}
									// SourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										}
									}
									// RepartitionSinkTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										}
									}
									// StateChangelogTopics
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													}
												}
											}
										}
									}
									// RepartitionSourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
					}
				}
				// Members
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
						m.Groups[i].Members = make([]StreamsGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
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
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								}
							}
							// TaskOffsets
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
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								}
							}
							// TaskEndOffsets
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
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
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
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].IsClassic = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]StreamsGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
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
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								}
							}
							// TaskOffsets
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
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								}
							}
							// TaskEndOffsets
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
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
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
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].IsClassic = val
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AuthorizedOperations = val
				}
			}
		} else {
			var err error
			length, err = protocol.ReadInt32(r)
			if err != nil {
				return err
			}
			m.Groups = make([]StreamsGroupDescribeResponseDescribedGroup, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Groups[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Groups[i].ErrorMessage = val
					}
				}
				// GroupId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupId = val
					}
				}
				// GroupState
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Groups[i].GroupState = val
					}
				}
				// GroupEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].GroupEpoch = val
				}
				// AssignmentEpoch
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AssignmentEpoch = val
				}
				// Topology
				if version >= 0 && version <= 999 {
					// Epoch
					if version >= 0 && version <= 999 {
						val, err := protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Topology.Epoch = val
					}
					// Subtopologies
					if version >= 0 && version <= 999 {
						var length int32
						if isFlexible {
							var lengthUint uint32
							lengthUint, err := protocol.ReadVaruint32(r)
							if err != nil {
								return err
							}
							if lengthUint == 0 {
								m.Groups[i].Topology.Subtopologies = nil
							} else {
								if lengthUint < 1 {
									return errors.New("invalid compact array length")
								}
								length = int32(lengthUint - 1)
								m.Groups[i].Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, length)
								for i := int32(0); i < length; i++ {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										}
									}
									// SourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										}
									}
									// RepartitionSinkTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										}
									}
									// StateChangelogTopics
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													}
												}
											}
										}
									}
									// RepartitionSourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
						} else {
							var err error
							length, err = protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							if length == -1 {
								m.Groups[i].Topology.Subtopologies = nil
							} else {
								m.Groups[i].Topology.Subtopologies = make([]StreamsGroupDescribeResponseSubtopology, length)
								for i := int32(0); i < length; i++ {
									// SubtopologyId
									if version >= 0 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										} else {
											val, err := protocol.ReadString(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SubtopologyId = val
										}
									}
									// SourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].SourceTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].SourceTopics[i] = val
												}
											}
										}
									}
									// RepartitionSinkTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										} else {
											var err error
											length, err = protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics = make([]string, length)
											for i := int32(0); i < length; i++ {
												if isFlexible {
													val, err := protocol.ReadCompactString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												} else {
													val, err := protocol.ReadString(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSinkTopics[i] = val
												}
											}
										}
									}
									// StateChangelogTopics
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].StateChangelogTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													}
												}
											}
										}
									}
									// RepartitionSourceTopics
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
											m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics = make([]TopicInfo, length)
											for i := int32(0); i < length; i++ {
												// Name
												if version >= 0 && version <= 999 {
													if isFlexible {
														val, err := protocol.ReadCompactString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													} else {
														val, err := protocol.ReadString(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Name = val
													}
												}
												// Partitions
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt32(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].Partitions = val
												}
												// ReplicationFactor
												if version >= 0 && version <= 999 {
													val, err := protocol.ReadInt16(r)
													if err != nil {
														return err
													}
													m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].ReplicationFactor = val
												}
												// TopicConfigs
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
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																}
															}
														}
													} else {
														var err error
														length, err = protocol.ReadInt32(r)
														if err != nil {
															return err
														}
														m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs = make([]KeyValue, length)
														for i := int32(0); i < length; i++ {
															// Key
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Key = val
																}
															}
															// Value
															if version >= 0 && version <= 999 {
																if isFlexible {
																	val, err := protocol.ReadCompactString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
																} else {
																	val, err := protocol.ReadString(r)
																	if err != nil {
																		return err
																	}
																	m.Groups[i].Topology.Subtopologies[i].RepartitionSourceTopics[i].TopicConfigs[i].Value = val
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
					}
				}
				// Members
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
						m.Groups[i].Members = make([]StreamsGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
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
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								}
							}
							// TaskOffsets
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
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								}
							}
							// TaskEndOffsets
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
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
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
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].IsClassic = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Groups[i].Members = make([]StreamsGroupDescribeResponseMember, length)
						for i := int32(0); i < length; i++ {
							// MemberId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].MemberId = val
								}
							}
							// MemberEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].MemberEpoch = val
							}
							// InstanceId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].InstanceId = val
								}
							}
							// RackId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].RackId = val
								}
							}
							// ClientId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientId = val
								}
							}
							// ClientHost
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientHost = val
								}
							}
							// TopologyEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].TopologyEpoch = val
							}
							// ProcessId
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ProcessId = val
								}
							}
							// UserEndpoint
							if version >= 0 && version <= 999 {
							}
							// ClientTags
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
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].ClientTags = make([]KeyValue, length)
									for i := int32(0); i < length; i++ {
										// Key
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Key = val
											}
										}
										// Value
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].ClientTags[i].Value = val
											}
										}
									}
								}
							}
							// TaskOffsets
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
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskOffsets[i].Offset = val
										}
									}
								}
							}
							// TaskEndOffsets
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
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Groups[i].Members[i].TaskEndOffsets = make([]TaskOffset, length)
									for i := int32(0); i < length; i++ {
										// SubtopologyId
										if version >= 0 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Groups[i].Members[i].TaskEndOffsets[i].SubtopologyId = val
											}
										}
										// Partition
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Partition = val
										}
										// Offset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Groups[i].Members[i].TaskEndOffsets[i].Offset = val
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
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Groups[i].Members[i].IsClassic = val
							}
						}
					}
				}
				// AuthorizedOperations
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt32(r)
					if err != nil {
						return err
					}
					m.Groups[i].AuthorizedOperations = val
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
}

// StreamsGroupDescribeResponseTopology represents The topology metadata currently initialized for the streams application. Can be null in case of a describe error..
type StreamsGroupDescribeResponseTopology struct {
	// The epoch of the currently initialized topology for this group.
	Epoch int32 `json:"epoch" versions:"0-999"`
	// The subtopologies of the streams application. This contains the configured subtopologies, where the number of partitions are set and any regular expressions are resolved to actual topics. Null if the group is uninitialized, source topics are missing or incorrectly partitioned.
	Subtopologies []StreamsGroupDescribeResponseSubtopology `json:"subtopologies" versions:"0-999"`
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
