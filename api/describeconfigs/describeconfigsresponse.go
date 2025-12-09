package describeconfigs

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeConfigsResponseApiKey        = 32
	DescribeConfigsResponseHeaderVersion = 1
)

// DescribeConfigsResponse represents a response message.
type DescribeConfigsResponse struct {
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
	// The results for each resource.
	Results []DescribeConfigsResponseDescribeConfigsResult `json:"results" versions:"0-999"`
}

// Encode encodes a DescribeConfigsResponse to a byte slice for the given version.
func (m *DescribeConfigsResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeConfigsResponse from a byte slice for the given version.
func (m *DescribeConfigsResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeConfigsResponse to an io.Writer for the given version.
func (m *DescribeConfigsResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
		isFlexible = true
	}

	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
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
			// ErrorMessage
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactNullableString(w, m.Results[i].ErrorMessage); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteNullableString(w, m.Results[i].ErrorMessage); err != nil {
						return err
					}
				}
			}
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(w, m.Results[i].ResourceType); err != nil {
					return err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Results[i].ResourceName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Results[i].ResourceName); err != nil {
						return err
					}
				}
			}
			// Configs
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Results[i].Configs) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Results[i].Configs))); err != nil {
						return err
					}
				}
				for i := range m.Results[i].Configs {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Results[i].Configs[i].Name); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Results[i].Configs[i].Name); err != nil {
								return err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Results[i].Configs[i].Value); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Results[i].Configs[i].Value); err != nil {
								return err
							}
						}
					}
					// ReadOnly
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(w, m.Results[i].Configs[i].ReadOnly); err != nil {
							return err
						}
					}
					// ConfigSource
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Results[i].Configs[i].ConfigSource); err != nil {
							return err
						}
					}
					// IsSensitive
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(w, m.Results[i].Configs[i].IsSensitive); err != nil {
							return err
						}
					}
					// Synonyms
					if version >= 1 && version <= 999 {
						if isFlexible {
							length := uint32(len(m.Results[i].Configs[i].Synonyms) + 1)
							if err := protocol.WriteVaruint32(w, length); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteInt32(w, int32(len(m.Results[i].Configs[i].Synonyms))); err != nil {
								return err
							}
						}
						for i := range m.Results[i].Configs[i].Synonyms {
							// Name
							if version >= 1 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(w, m.Results[i].Configs[i].Synonyms[i].Name); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteString(w, m.Results[i].Configs[i].Synonyms[i].Name); err != nil {
										return err
									}
								}
							}
							// Value
							if version >= 1 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(w, m.Results[i].Configs[i].Synonyms[i].Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(w, m.Results[i].Configs[i].Synonyms[i].Value); err != nil {
										return err
									}
								}
							}
							// Source
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(w, m.Results[i].Configs[i].Synonyms[i].Source); err != nil {
									return err
								}
							}
						}
					}
					// ConfigType
					if version >= 3 && version <= 999 {
						if err := protocol.WriteInt8(w, m.Results[i].Configs[i].ConfigType); err != nil {
							return err
						}
					}
					// Documentation
					if version >= 3 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(w, m.Results[i].Configs[i].Documentation); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteNullableString(w, m.Results[i].Configs[i].Documentation); err != nil {
								return err
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

// Read reads a DescribeConfigsResponse from an io.Reader for the given version.
func (m *DescribeConfigsResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 4 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 4 {
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
			m.Results = make([]DescribeConfigsResponseDescribeConfigsResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					}
				}
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Results[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].ResourceName = val
					}
				}
				// Configs
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
						m.Results[i].Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
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
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Results[i].Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
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
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
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
			m.Results = make([]DescribeConfigsResponseDescribeConfigsResult, length)
			for i := int32(0); i < length; i++ {
				// ErrorCode
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt16(r)
					if err != nil {
						return err
					}
					m.Results[i].ErrorCode = val
				}
				// ErrorMessage
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					} else {
						val, err := protocol.ReadNullableString(r)
						if err != nil {
							return err
						}
						m.Results[i].ErrorMessage = val
					}
				}
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					m.Results[i].ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Results[i].ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Results[i].ResourceName = val
					}
				}
				// Configs
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
						m.Results[i].Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
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
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Results[i].Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, length)
						for i := int32(0); i < length; i++ {
							// Name
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Name = val
								}
							}
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
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
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								} else {
									var err error
									length, err = protocol.ReadInt32(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, length)
									for i := int32(0); i < length; i++ {
										// Name
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											} else {
												val, err := protocol.ReadString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Name = val
											}
										}
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												m.Results[i].Configs[i].Synonyms[i].Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											m.Results[i].Configs[i].Synonyms[i].Source = val
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								m.Results[i].Configs[i].ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									m.Results[i].Configs[i].Documentation = val
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

// DescribeConfigsResponseDescribeConfigsResult represents The results for each resource..
type DescribeConfigsResponseDescribeConfigsResult struct {
	// The error code, or 0 if we were able to successfully describe the configurations.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The error message, or null if we were able to successfully describe the configurations.
	ErrorMessage *string `json:"errormessage" versions:"0-999"`
	// The resource type.
	ResourceType int8 `json:"resourcetype" versions:"0-999"`
	// The resource name.
	ResourceName string `json:"resourcename" versions:"0-999"`
	// Each listed configuration.
	Configs []DescribeConfigsResponseDescribeConfigsResourceResult `json:"configs" versions:"0-999"`
}

// DescribeConfigsResponseDescribeConfigsResourceResult represents Each listed configuration..
type DescribeConfigsResponseDescribeConfigsResourceResult struct {
	// The configuration name.
	Name string `json:"name" versions:"0-999"`
	// The configuration value.
	Value *string `json:"value" versions:"0-999"`
	// True if the configuration is read-only.
	ReadOnly bool `json:"readonly" versions:"0-999"`
	// The configuration source.
	ConfigSource int8 `json:"configsource" versions:"1-999"`
	// True if this configuration is sensitive.
	IsSensitive bool `json:"issensitive" versions:"0-999"`
	// The synonyms for this configuration key.
	Synonyms []DescribeConfigsResponseDescribeConfigsSynonym `json:"synonyms" versions:"1-999"`
	// The configuration data type. Type can be one of the following values - BOOLEAN, STRING, INT, SHORT, LONG, DOUBLE, LIST, CLASS, PASSWORD.
	ConfigType int8 `json:"configtype" versions:"3-999"`
	// The configuration documentation.
	Documentation *string `json:"documentation" versions:"3-999"`
}

// DescribeConfigsResponseDescribeConfigsSynonym represents The synonyms for this configuration key..
type DescribeConfigsResponseDescribeConfigsSynonym struct {
	// The synonym name.
	Name string `json:"name" versions:"1-999"`
	// The synonym value.
	Value *string `json:"value" versions:"1-999"`
	// The synonym source.
	Source int8 `json:"source" versions:"1-999"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsResponse.
func (m *DescribeConfigsResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsResponse.
func (m *DescribeConfigsResponse) readTaggedFields(r io.Reader, version int16) error {
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
