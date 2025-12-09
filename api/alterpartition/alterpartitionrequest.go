package alterpartition

import (
	"bytes"
	"errors"
	"io"

	"github.com/google/uuid"
	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	AlterPartitionRequestApiKey        = 56
	AlterPartitionRequestHeaderVersion = 1
)

// AlterPartitionRequest represents a request message.
type AlterPartitionRequest struct {
	// The ID of the requesting broker.
	BrokerId int32 `json:"brokerid" versions:"0-999"`
	// The epoch of the requesting broker.
	BrokerEpoch int64 `json:"brokerepoch" versions:"0-999"`
	// The topics to alter ISRs for.
	Topics []AlterPartitionRequestTopicData `json:"topics" versions:"0-999"`
}

// Encode encodes a AlterPartitionRequest to a byte slice for the given version.
func (m *AlterPartitionRequest) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a AlterPartitionRequest from a byte slice for the given version.
func (m *AlterPartitionRequest) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a AlterPartitionRequest to an io.Writer for the given version.
func (m *AlterPartitionRequest) Write(w io.Writer, version int16) error {
	if version < 2 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.BrokerId); err != nil {
			return err
		}
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt64(w, m.BrokerEpoch); err != nil {
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
			if version >= 2 && version <= 999 {
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
					// LeaderEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].LeaderEpoch); err != nil {
							return err
						}
					}
					// NewIsr
					if version >= 0 && version <= 2 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].NewIsr) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].NewIsr))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].NewIsr {
							if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].NewIsr[i]); err != nil {
								return err
							}
							_ = i
						}
					}
					// NewIsrWithEpochs
					if version >= 3 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Topics[i].Partitions[i].NewIsrWithEpochs) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Topics[i].Partitions[i].NewIsrWithEpochs))); err != nil {
								return err
							}
						}
						for i := range m.Topics[i].Partitions[i].NewIsrWithEpochs {
							// BrokerId
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId); err != nil {
									return err
								}
							}
							// BrokerEpoch
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt64(w, m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch); err != nil {
									return err
								}
							}
						}
					}
					// LeaderRecoveryState
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Topics[i].Partitions[i].LeaderRecoveryState); err != nil {
							return err
						}
					}
					// PartitionEpoch
					if version >= 0 && version <= 999 {
						if err := protocol.WriteInt32(w, m.Topics[i].Partitions[i].PartitionEpoch); err != nil {
							return err
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

// Read reads a AlterPartitionRequest from an io.Reader for the given version.
func (m *AlterPartitionRequest) Read(r io.Reader, version int16) error {
	if version < 2 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 0 {
		isFlexible = true
	}

	// BrokerId
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.BrokerId = val
	}
	// BrokerEpoch
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt64(r)
		if err != nil {
			return err
		}
		m.BrokerEpoch = val
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
			m.Topics = make([]AlterPartitionRequestTopicData, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 2 && version <= 999 {
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
						m.Topics[i].Partitions = make([]AlterPartitionRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
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
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
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
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionEpoch = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]AlterPartitionRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
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
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
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
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionEpoch = val
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
			m.Topics = make([]AlterPartitionRequestTopicData, length)
			for i := int32(0); i < length; i++ {
				// TopicId
				if version >= 2 && version <= 999 {
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
						m.Topics[i].Partitions = make([]AlterPartitionRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
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
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
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
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionEpoch = val
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Topics[i].Partitions = make([]AlterPartitionRequestPartitionData, length)
						for i := int32(0); i < length; i++ {
							// PartitionIndex
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionIndex = val
							}
							// LeaderEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderEpoch = val
							}
							// NewIsr
							if version >= 0 && version <= 2 {
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
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsr = make([]int32, length)
									for i := int32(0); i < length; i++ {
										val, err := protocol.ReadInt32(r)
										if err != nil {
											return err
										}
										m.Topics[i].Partitions[i].NewIsr[i] = val
									}
								}
							}
							// NewIsrWithEpochs
							if version >= 3 && version <= 999 {
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
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Topics[i].Partitions[i].NewIsrWithEpochs = make([]AlterPartitionRequestBrokerState, length)
									for i := int32(0); i < length; i++ {
										// BrokerId
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt32(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerId = val
										}
										// BrokerEpoch
										if version >= 3 && version <= 999 {
											val, err := protocol.ReadInt64(r)
											if err != nil {
												return err
											}
											m.Topics[i].Partitions[i].NewIsrWithEpochs[i].BrokerEpoch = val
										}
									}
								}
							}
							// LeaderRecoveryState
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].LeaderRecoveryState = val
							}
							// PartitionEpoch
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadInt32(r)
								if err != nil {
									return err
								}
								m.Topics[i].Partitions[i].PartitionEpoch = val
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

// AlterPartitionRequestTopicData represents The topics to alter ISRs for..
type AlterPartitionRequestTopicData struct {
	// The ID of the topic to alter ISRs for.
	TopicId uuid.UUID `json:"topicid" versions:"2-999"`
	// The partitions to alter ISRs for.
	Partitions []AlterPartitionRequestPartitionData `json:"partitions" versions:"0-999"`
}

// AlterPartitionRequestPartitionData represents The partitions to alter ISRs for..
type AlterPartitionRequestPartitionData struct {
	// The partition index.
	PartitionIndex int32 `json:"partitionindex" versions:"0-999"`
	// The leader epoch of this partition.
	LeaderEpoch int32 `json:"leaderepoch" versions:"0-999"`
	// The ISR for this partition. Deprecated since version 3.
	NewIsr []int32 `json:"newisr" versions:"0-2"`
	// The ISR for this partition.
	NewIsrWithEpochs []AlterPartitionRequestBrokerState `json:"newisrwithepochs" versions:"3-999"`
	// 1 if the partition is recovering from an unclean leader election; 0 otherwise.
	LeaderRecoveryState int8 `json:"leaderrecoverystate" versions:"1-999"`
	// The expected epoch of the partition which is being updated.
	PartitionEpoch int32 `json:"partitionepoch" versions:"0-999"`
}

// AlterPartitionRequestBrokerState represents The ISR for this partition..
type AlterPartitionRequestBrokerState struct {
	// The ID of the broker.
	BrokerId int32 `json:"brokerid" versions:"3-999"`
	// The epoch of the broker. It will be -1 if the epoch check is not supported.
	BrokerEpoch int64 `json:"brokerepoch" versions:"3-999"`
}

// writeTaggedFields writes tagged fields for AlterPartitionRequest.
func (m *AlterPartitionRequest) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for AlterPartitionRequest.
func (m *AlterPartitionRequest) readTaggedFields(r io.Reader, version int16) error {
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
