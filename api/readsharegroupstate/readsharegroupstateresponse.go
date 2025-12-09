package readsharegroupstate

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	ReadShareGroupStateResponseApiKey        = 84
	ReadShareGroupStateResponseHeaderVersion = 1
)

// ReadShareGroupStateResponse represents a response message.
type ReadShareGroupStateResponse struct {
	// The read results.
	Results []ReadShareGroupStateResponseReadStateResult `json:"results" versions:"0-999"`
}

// Encode encodes a ReadShareGroupStateResponse to a byte slice for the given version.
func (m *ReadShareGroupStateResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a ReadShareGroupStateResponse from a byte slice for the given version.
func (m *ReadShareGroupStateResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a ReadShareGroupStateResponse to an io.Writer for the given version.
func (m *ReadShareGroupStateResponse) Write(w io.Writer, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
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
			// TopicId
			if version >= 0 && version <= 999 {
				if err := protocol.WriteUUID(w, m.Results[i].TopicId); err != nil {
					return err
				}
			}
			// Partitions
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Results[i].Partitions) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Results[i].Partitions))); err != nil {
						return err
					}
				}
				for i := range m.Results[i].Partitions {
					// Partition
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Results[i].Partitions[i].Partition); err != nil {
							return err
						}
					}
					// ErrorCode
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt16(w, m.Results[i].Partitions[i].ErrorCode); err != nil {
							return err
						}
					}
					// ErrorMessage
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Results[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Results[i].Partitions[i].ErrorMessage); err != nil {
								return err
							}
						}
					}
					// StateEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Results[i].Partitions[i].StateEpoch); err != nil {
							return err
						}
					}
					// StartOffset
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt64(w, m.Results[i].Partitions[i].StartOffset); err != nil {
							return err
						}
					}
					// StateBatches
					if version >= 0 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Results[i].Partitions[i].StateBatches) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Results[i].Partitions[i].StateBatches))); err != nil {
								return err
							}
						}
						for i := range m.Results[i].Partitions[i].StateBatches {
							// FirstOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Results[i].Partitions[i].StateBatches[i].FirstOffset); err != nil {
									return err
								}
							}
							// LastOffset
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Results[i].Partitions[i].StateBatches[i].LastOffset); err != nil {
									return err
								}
							}
							// DeliveryState
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt8(w, m.Results[i].Partitions[i].StateBatches[i].DeliveryState); err != nil {
									return err
								}
							}
							// DeliveryCount
							if version >= 0 && version <= 999 {
								if err := protocol.WriteInt16(w, m.Results[i].Partitions[i].StateBatches[i].DeliveryCount); err != nil {
									return err
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

// Read reads a ReadShareGroupStateResponse from an io.Reader for the given version.
func (m *ReadShareGroupStateResponse) Read(r io.Reader, version int16) error {
	if version < 0 || version > 0 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
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
			m.Results = make([]ReadShareGroupStateResponseReadStateResult, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Results[i].TopicId = val
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
						m.Results[i].Partitions = make([]ReadShareGroupStateResponsePartitionResult, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].Partition = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								}
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StartOffset = val
							}
							// StateBatches
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
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
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
						m.Results[i].Partitions = make([]ReadShareGroupStateResponsePartitionResult, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].Partition = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								}
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StartOffset = val
							}
							// StateBatches
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
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
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
			m.Results = make([]ReadShareGroupStateResponseReadStateResult, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadUUID(r)
					if err != nil {
						return err
					}
					m.Results[i].TopicId = val
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
						m.Results[i].Partitions = make([]ReadShareGroupStateResponsePartitionResult, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].Partition = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								}
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StartOffset = val
							}
							// StateBatches
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
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
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
						m.Results[i].Partitions = make([]ReadShareGroupStateResponsePartitionResult, length)
						for i := int32(0); i < length; i++ {
							// Partition
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].Partition = val
							}
							// ErrorCode
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt16(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].ErrorCode = val
							}
							// ErrorMessage
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].ErrorMessage = val
								}
							}
							// StateEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StateEpoch = val
							}
							// StartOffset
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt64(r)
								if err != nil {
									return err
								}
								m.Results[i].Partitions[i].StartOffset = val
							}
							// StateBatches
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
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Partitions[i].StateBatches = make([]ReadShareGroupStateResponseStateBatch, length)
									for i := int32(0); i < length; i++ {
										// FirstOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].FirstOffset = val
										}
										// LastOffset
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].LastOffset = val
										}
										// DeliveryState
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryState = val
										}
										// DeliveryCount
										if version >= 0 && version <= 999 {
											val, err := protocol.ReadInt16(r)
											if err != nil {
												return err
											}
											m.Results[i].Partitions[i].StateBatches[i].DeliveryCount = val
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

// ReadShareGroupStateResponseReadStateResult represents The read results..
type ReadShareGroupStateResponseReadStateResult struct {
	// The topic identifier.
	TopicId uuid.UUID `json:"topicid" versions:"0-999"`
	// The results for the partitions.
	Partitions []ReadShareGroupStateResponsePartitionResult `json:"partitions" versions:"0-999"`
}

// ReadShareGroupStateResponsePartitionResult represents The results for the partitions..
type ReadShareGroupStateResponsePartitionResult struct {
	// The partition index.
	Partition int32 `json:"partition" versions:"0-999"`
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if there was no error.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The state epoch of the share-partition.
	StateEpoch int32 `json:"stateepoch" versions:"0-999"`
	// The share-partition start offset, which can be -1 if it is not yet initialized.
	StartOffset int64 `json:"startoffset" versions:"0-999"`
	// The state batches for this share-partition.
	StateBatches []ReadShareGroupStateResponseStateBatch `json:"statebatches" versions:"0-999"`
}

// ReadShareGroupStateResponseStateBatch represents The state batches for this share-partition..
type ReadShareGroupStateResponseStateBatch struct {
	// The first offset of this state batch.
	FirstOffset int64 `json:"firstoffset" versions:"0-999"`
	// The last offset of this state batch.
	LastOffset int64 `json:"lastoffset" versions:"0-999"`
	// The delivery state - 0:Available,2:Acked,4:Archived.
	DeliveryState int8 `json:"deliverystate" versions:"0-999"`
	// The delivery count.
	DeliveryCount int16 `json:"deliverycount" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for ReadShareGroupStateResponse.
func (m *ReadShareGroupStateResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for ReadShareGroupStateResponse.
func (m *ReadShareGroupStateResponse) readTaggedFields(r io.Reader, version int16) error {
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
