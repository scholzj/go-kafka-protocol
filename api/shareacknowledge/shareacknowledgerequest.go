package shareacknowledge

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ShareAcknowledgeRequestApiKey        = 79
	ShareAcknowledgeRequestHeaderVersion = 1
)

// ShareAcknowledgeRequest represents a request message.
type ShareAcknowledgeRequest struct {
	// The group identifier.
	GroupId *string `json:"groupid" versions:"0-999"`
	// The member ID.
	MemberId *string `json:"memberid" versions:"0-999"`
	// The current share session epoch: 0 to open a share session; -1 to close it; otherwise increments for consecutive requests.
	ShareSessionEpoch int32 `json:"sharesessionepoch" versions:"0-999"`
	// Whether Renew type acknowledgements present in AcknowledgementBatches.
	IsRenewAck bool `json:"isrenewack" versions:"2-999"`
	// The topics containing records to acknowledge.
	Topics []ShareAcknowledgeRequestAcknowledgeTopic `json:"topics" versions:"0-999"`
}

// Encode encodes a ShareAcknowledgeRequest to a byte slice for the given version.
func (m *ShareAcknowledgeRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ShareAcknowledgeRequest from a byte slice for the given version.
func (m *ShareAcknowledgeRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ShareAcknowledgeRequest to an io.Writer for the given version.
func (m *ShareAcknowledgeRequest) Write(w io.Writer, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.GroupId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.GroupId); err != nil {
				return err
			}
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			if err := protocol.WriteCompactNullableString(w, m.MemberId); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteNullableString(w, m.MemberId); err != nil {
				return err
			}
		}
	}
	// ShareSessionEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ShareSessionEpoch); err != nil {
			return err
		}
	}
	// IsRenewAck
	if version >= 2 && version <= 999 {
		if err := protocol.WriteBool(w, m.IsRenewAck); err != nil {
			return err
		}
	}
	// Topics
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Topics) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Topics))); err != nil {
				return err
			}
		}
		for i := range m.Topics {
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Topics[i].TopicId); err != nil {
					return err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Topics[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Topics[i].Partitions {
					// PartitionIndex
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionIndex); err != nil {
							return err
						}
					}
					// AcknowledgementBatches
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].AcknowledgementBatches) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].AcknowledgementBatches))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].AcknowledgementBatches {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset); err != nil {
									return err
								}
							}
							// AcknowledgeTypes
							if version >= 0 && version <= 999 {
								if isFlexible {
									length := uint32(len(m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes) + 1)
									if err := protocol.WriteVaruint32(w, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes))); err != nil {
										return err
									}
								}
								for i := range m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes {
									if err := protocol.WriteInt8(w, m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i]); err != nil {
										return err
									}
									_ = i
								}
							}
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

// Read reads a ShareAcknowledgeRequest from an io.Reader for the given version.
func (m *ShareAcknowledgeRequest) Read(r io.Reader, version int16) error {
	if version < 1 || version > 2 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// GroupId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.GroupId = val
		}
	}
	// MemberId
	if version >= 0 && version <= 999 {
		if isFlexible {
			val, err := protocol.ReadCompactNullableString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		} else {
			val, err := protocol.ReadNullableString(r)
			if err != nil {
				return err
			}
			m.MemberId = val
		}
	}
	// ShareSessionEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ShareSessionEpoch = val
	}
	// IsRenewAck
	if version >= 2 && version <= 999 {
		val, err := protocol.ReadBool(r)
		if err != nil {
			return err
		}
		m.IsRenewAck = val
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
			m.Topics = make([]ShareAcknowledgeRequestAcknowledgeTopic, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
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
						m.Topics[i].Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// AcknowledgementBatches
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
						m.Topics[i].Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// AcknowledgementBatches
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
			m.Topics = make([]ShareAcknowledgeRequestAcknowledgeTopic, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Topics[i].TopicId = val
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
						m.Topics[i].Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// AcknowledgementBatches
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
						m.Topics[i].Partitions = make([]ShareAcknowledgeRequestAcknowledgePartition, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// AcknowledgementBatches
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
									m.Topics[i].Partitions[i].AcknowledgementBatches = make([]ShareAcknowledgeRequestAcknowledgementBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].AcknowledgementBatches[i].LastOffset = val
										}
										// AcknowledgeTypes
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
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
												}
											} else {
												var err error
												length, err = protocol.ReadInt32(r)
												if err != nil {
													return err
												}
												m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes = make([]int8, length)
												for i := int32(0); i < length; i++ {
													val, err := protocol.ReadInt8(r)
													if err != nil {
														return err
													}
													m.Topics[i].Partitions[i].AcknowledgementBatches[i].AcknowledgeTypes[i] = val
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
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// ShareAcknowledgeRequestAcknowledgeTopic represents The topics containing records to acknowledge..
type ShareAcknowledgeRequestAcknowledgeTopic struct {
	// The unique topic ID.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The partitions containing records to acknowledge.
	Partitions []ShareAcknowledgeRequestAcknowledgePartition `json:"partitions" versions:"0-999"`
}

// ShareAcknowledgeRequestAcknowledgePartition represents The partitions containing records to acknowledge..
type ShareAcknowledgeRequestAcknowledgePartition struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// Record batches to acknowledge.
	AcknowledgementBatches []ShareAcknowledgeRequestAcknowledgementBatch `json:"acknowledgementbatches" versions:"0-999"`
}

// ShareAcknowledgeRequestAcknowledgementBatch represents Record batches to acknowledge..
type ShareAcknowledgeRequestAcknowledgementBatch struct {
	// First offset of batch of records to acknowledge.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// Last offset (inclusive) of batch of records to acknowledge.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// Array of acknowledge types - 0:Gap,1:Accept,2:Release,3:Reject,4:Renew.
	AcknowledgeTypes []int8 `json:"acknowledgetypes" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ShareAcknowledgeRequest.
func (m *ShareAcknowledgeRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ShareAcknowledgeRequest.
func (m *ShareAcknowledgeRequest) readTaggedFields(r io.Reader, version int16) error {
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
