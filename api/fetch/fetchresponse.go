package fetch

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	FetchResponseApiKey        = 1
	FetchResponseHeaderVersion = 1
)

// FetchResponse represents a response message.
type FetchResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"1-999"`
	// The top level response error code.
	ErrorCode int16 `json:"errorcode" versions:"7-999"`
	// The fetch session ID, or 0 if this is not part of a fetch session.
	SessionId int32 `json:"sessionid" versions:"7-999"`
	// The response topics.
	Responses []FetchResponseFetchableTopicResponse `json:"responses" versions:"0-999"`
	// Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH.
	NodeEndpoints []FetchResponseNodeEndpoint `json:"nodeendpoints" versions:"16-999" tag:"0"`
}

// Encode encodes a FetchResponse to a byte slice for the given version.
func (m *FetchResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a FetchResponse from a byte slice for the given version.
func (m *FetchResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a FetchResponse to an io.Writer for the given version.
func (m *FetchResponse) Write(w io.Writer, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
		}
	}
	// ErrorCode
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// SessionId
	if version >= 7 && version <= 999 {
		if err := protocol.WriteInt32(w, m.SessionId); err != nil {
			return err
		}
	}
	// Responses
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Responses) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Responses))); err != nil {
				return err
			}
		}
		for i := range m.Responses {
			// Topic
			if version >= 0 && version <= 12 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Responses[i].Topic); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Responses[i].Topic); err != nil {
						return err
					}
				}
			}
			// TopicId
			if version >= 13 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Responses[i].TopicId); err != nil {
					return err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Responses[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Responses[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Responses[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Responses[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// HighWatermark
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Responses[i].Partitions[i].HighWatermark); err != nil {
							return err
						}
					}
					// LastStableOffset
					if version >= 4 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Responses[i].Partitions[i].LastStableOffset); err != nil {
							return err
						}
					}
					// LogStartOffset
					if version >= 5 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Responses[i].Partitions[i].LogStartOffset); err != nil {
							return err
						}
					}
					// DivergingEpoch
					if version >= 12 && version <= 999 {
					}
					// CurrentLeader
					if version >= 12 && version <= 999 {
					}
					// SnapshotId
					if version >= 12 && version <= 999 {
					}
					// AbortedTransactions
					if version >= 4 && version <= 999 {
						if m.Responses[i].Partitions[i].AbortedTransactions == nil {
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
								length := uint32(len(m.Responses[i].Partitions[i].AbortedTransactions) + 1)
								if err := protocol.WriteVaruint32(w, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(w, int32(len(m.Responses[i].Partitions[i].AbortedTransactions))); err != nil {
									return err
								}
							}
							for i := range m.Responses[i].Partitions[i].AbortedTransactions {
								// ProducerId
								if version >= 4 && version <= 999 {
									if err := protocol.WriteInt64(w, m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId); err != nil {
										return err
									}
								}
								// FirstOffset
								if version >= 4 && version <= 999 {
									if err := protocol.WriteInt64(w, m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset); err != nil {
										return err
									}
								}
							}
						}
					}
					// PreferredReadReplica
					if version >= 11 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Responses[i].Partitions[i].PreferredReadReplica); err != nil {
							return err
						}
					}
					// Records
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableBytes(w, m.Responses[i].Partitions[i].Records); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableBytes(w, m.Responses[i].Partitions[i].Records); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
	if version >= 16 && version <= 999 {
	}
	// Write tagged fields if flexible
	if isFlexible {
		if err := m.writeTaggedFields(w, version); err != nil {
			return err
		}
	}
	return nil
}

// Read reads a FetchResponse from an io.Reader for the given version.
func (m *FetchResponse) Read(r io.Reader, version int16) error {
	if version < 4 || version > 18 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 12 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 1 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// ErrorCode
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// SessionId
	if version >= 7 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.SessionId = val
	}
	// Responses
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
			m.Responses = make([]FetchResponseFetchableTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Topic = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
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
						m.Responses[i].Partitions = make([]FetchResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
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
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PreferredReadReplica = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]FetchResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
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
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PreferredReadReplica = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
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
			m.Responses = make([]FetchResponseFetchableTopicResponse, length)
			for i := int32(0); i < length; i++ {
				// Topic
				if version >= 0 && version <= 12 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Topic = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Responses[i].Topic = val
					}
				}
				// TopicId
				if version >= 13 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Responses[i].TopicId = val
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
						m.Responses[i].Partitions = make([]FetchResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
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
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PreferredReadReplica = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Responses[i].Partitions = make([]FetchResponsePartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PartitionIndex = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].ErrorCode = val
							}
							// HighWatermark
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].HighWatermark = val
							}
							// LastStableOffset
							if version >= 4 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LastStableOffset = val
							}
							// LogStartOffset
							if version >= 5 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].LogStartOffset = val
							}
							// DivergingEpoch
							if version >= 12 && version <= 999 {
							}
							// CurrentLeader
							if version >= 12 && version <= 999 {
							}
							// SnapshotId
							if version >= 12 && version <= 999 {
							}
							// AbortedTransactions
							if version >= 4 && version <= 999 {
								var length int32
								if isFlexible {
									var lengthUint uint32
									lengthUint, err := protocol.ReadVaruint32(r)
									if err != nil {
										return err
									}
									if lengthUint == 0 {
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										if lengthUint < 1 {
											return errors.New("invalid compact array length")
										}
										length = int32(lengthUint - 1)
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
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
										m.Responses[i].Partitions[i].AbortedTransactions = nil
									} else {
										m.Responses[i].Partitions[i].AbortedTransactions = make([]FetchResponseAbortedTransaction, length)
										for i := int32(0); i < length; i++ {
											// ProducerId
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].ProducerId = val
											}
											// FirstOffset
											if version >= 4 && version <= 999 {
												val, err := protocol.ReadInt64(r)
												if err != nil {
													return err
												}
												m.Responses[i].Partitions[i].AbortedTransactions[i].FirstOffset = val
											}
										}
									}
								}
							}
							// PreferredReadReplica
							if version >= 11 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Responses[i].Partitions[i].PreferredReadReplica = val
							}
							// Records
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								} else {
									val, err := protocol.ReadNullableBytes(r)
									if err != nil {
										return err
									}
									m.Responses[i].Partitions[i].Records = val
								}
							}
						}
					}
				}
			}
		}
	}
	// NodeEndpoints
	if version >= 16 && version <= 999 {
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// FetchResponseFetchableTopicResponse represents The response topics..
type FetchResponseFetchableTopicResponse struct {
	// The topic name.
	Topic string `json:"topic" versions:"0-12"`
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"13-999"`
	// The topic partitions.
	Partitions []FetchResponsePartitionData `json:"partitions" versions:"0-999"`
}

// FetchResponsePartitionData represents The topic partitions..
type FetchResponsePartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The error code, or 0 if there was no fetch error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The current high water mark.
	HighWatermark int64 `json:"highwatermark" versions:"0-999"`
	// The last stable offset (or LSO) of the partition. This is the last offset such that the state of all transactional records prior to this offset have been decided (ABORTED or COMMITTED).
	LastStableOffset int64 `json:"laststableoffset" versions:"4-999"`
	// The current log start offset.
	LogStartOffset int64 `json:"logstartoffset" versions:"5-999"`
	// In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge.
	DivergingEpoch FetchResponseEpochEndOffset `json:"divergingepoch" versions:"12-999" tag:"0"`
	// The current leader of the partition.
	CurrentLeader FetchResponseLeaderIdAndEpoch `json:"currentleader" versions:"12-999" tag:"1"`
	// In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request.
	SnapshotId FetchResponseSnapshotId `json:"snapshotid" versions:"12-999" tag:"2"`
	// The aborted transactions.
	AbortedTransactions []FetchResponseAbortedTransaction `json:"abortedtransactions" versions:"4-999"`
	// The preferred read replica for the consumer to use on its next fetch request.
	PreferredReadReplica int32 `json:"preferredreadreplica" versions:"11-999"`
	// The record data.
	Records *[]byte `json:"records" versions:"0-999"`
}

// FetchResponseEpochEndOffset represents In case divergence is detected based on the `LastFetchedEpoch` and `FetchOffset` in the request, this field indicates the largest epoch and its end offset such that subsequent records are known to diverge..
type FetchResponseEpochEndOffset struct {
	// The largest epoch.
	Epoch int32 `json:"epoch" versions:"12-999"`
	// The end offset of the epoch.
	EndOffset int64 `json:"endoffset" versions:"12-999"`
}

// FetchResponseLeaderIdAndEpoch represents The current leader of the partition..
type FetchResponseLeaderIdAndEpoch struct {
	// The ID of the current leader or -1 if the leader is unknown.
	LeaderId int32 `json:"leaderid" versions:"12-999"`
	// The latest known leader epoch.
	LeaderEpoch int32 `json:"leaderepoch" versions:"12-999"`
}

// FetchResponseSnapshotId represents In the case of fetching an offset less than the LogStartOffset, this is the end offset and epoch that should be used in the FetchSnapshot request..
type FetchResponseSnapshotId struct {
	// The end offset of the epoch.
	EndOffset int64 `json:"endoffset" versions:"0-999"`
	// The largest epoch.
	Epoch int32 `json:"epoch" versions:"0-999"`
}

// FetchResponseAbortedTransaction represents The aborted transactions..
type FetchResponseAbortedTransaction struct {
	// The producer id associated with the aborted transaction.
	ProducerId int64 `json:"producerid" versions:"4-999"`
	// The first offset in the aborted transaction.
	FirstOffset int64 `json:"firstoffset" versions:"4-999"`
}

// FetchResponseNodeEndpoint represents Endpoints for all current-leaders enumerated in PartitionData, with errors NOT_LEADER_OR_FOLLOWER & FENCED_LEADER_EPOCH..
type FetchResponseNodeEndpoint struct {
	// The ID of the associated node.
	NodeId int32 `json:"nodeid" versions:"16-999"`
	// The node's hostname.
	Host string `json:"host" versions:"16-999"`
	// The node's port.
	Port int32 `json:"port" versions:"16-999"`
	// The rack of the node, or null if it has not been assigned to a rack.
	Rack *string `json:"rack" versions:"16-999"`
}

// writeTaggedFields writes tagged fields for FetchResponse.
func (m *FetchResponse) writeTaggedFields(w io.Writer, version int16) error {
	var taggedFieldsCount int
	var taggedFieldsBuf bytes.Buffer

	isFlexible := version >= 12

	// NodeEndpoints (tag 0)
	if version >= 16 {
		if m.NodeEndpoints != nil && len(m.NodeEndpoints) > 0 {
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, uint32(0)); err != nil {
				return err
			}
			// Array in tagged field
			length := uint32(len(m.NodeEndpoints) + 1)
			if err := protocol.WriteVaruint32(&taggedFieldsBuf, length); err != nil {
				return err
			}
			for i := range m.NodeEndpoints {
				// NodeId
				if version >= 16 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].NodeId); err != nil {
						return err
					}
				}
				// Host
				if version >= 16 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(w, m.NodeEndpoints[i].Host); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(w, m.NodeEndpoints[i].Host); err != nil {
							return err
						}
					}
				}
				// Port
				if version >= 16 && version <= 999 {
					if err := protocol.WriteInt32(w, m.NodeEndpoints[i].Port); err != nil {
						return err
					}
				}
				// Rack
				if version >= 16 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteNullableString(w, m.NodeEndpoints[i].Rack); err != nil {
							return err
						}
					}
				}
			}
			taggedFieldsCount++
		}
	}

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

// readTaggedFields reads tagged fields for FetchResponse.
func (m *FetchResponse) readTaggedFields(r io.Reader, version int16) error {
	isFlexible := version >= 12

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
		case 0: // NodeEndpoints
			if version >= 16 {
				// Array in tagged field
				length, err := protocol.ReadVaruint32(r)
				if err != nil {
					return err
				}
				if length == 0 {
					m.NodeEndpoints = nil
				} else {
					if length < 1 {
						return errors.New("invalid compact array length")
					}
					m.NodeEndpoints = make([]FetchResponseNodeEndpoint, length-1)
					for i := uint32(0); i < length-1; i++ {
						// NodeId
						if version >= 16 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].NodeId = val
						}
						// Host
						if version >= 16 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Host = val
							} else {
								val, err := protocol.ReadString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Host = val
							}
						}
						// Port
						if version >= 16 && version <= 999 {
							val, err := protocol.ReadInt32(r)
							if err != nil {
								return err
							}
							m.NodeEndpoints[i].Port = val
						}
						// Rack
						if version >= 16 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Rack = val
							} else {
								val, err := protocol.ReadNullableString(r)
								if err != nil {
									return err
								}
								m.NodeEndpoints[i].Rack = val
							}
						}
					}
				}
			}
		default:
			// Unknown tag, skip it
			// Read and discard the field data
			// For now, we'll need to know the type to skip properly
			// This is a simplified implementation
		}
	}

	return nil
}
