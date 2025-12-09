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
		// Encode array using ArrayEncoder
		encoder := func(item interface{}) ([]byte, error) {
			if item == nil {
				return nil, nil
			}
			structItem, ok := item.(DescribeConfigsResponseDescribeConfigsResult)
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
			// ResourceType
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt8(elemW, structItem.ResourceType); err != nil {
					return nil, err
				}
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteString(elemW, structItem.ResourceName); err != nil {
						return nil, err
					}
				}
			}
			// Configs
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(structItem.Configs) + 1)
					if err := protocol.WriteVaruint32(elemW, length); err != nil {
						return nil, err
					}
				} else {
					if err := protocol.WriteInt32(elemW, int32(len(structItem.Configs))); err != nil {
						return nil, err
					}
				}
				for i := range structItem.Configs {
					// Name
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(elemW, structItem.Configs[i].Name); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteString(elemW, structItem.Configs[i].Name); err != nil {
								return nil, err
							}
						}
					}
					// Value
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Configs[i].Value); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Configs[i].Value); err != nil {
								return nil, err
							}
						}
					}
					// ReadOnly
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(elemW, structItem.Configs[i].ReadOnly); err != nil {
							return nil, err
						}
					}
					// ConfigSource
					if version >= 1 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.Configs[i].ConfigSource); err != nil {
							return nil, err
						}
					}
					// IsSensitive
					if version >= 0 && version <= 999 {
						if err := protocol.WriteBool(elemW, structItem.Configs[i].IsSensitive); err != nil {
							return nil, err
						}
					}
					// Synonyms
					if version >= 1 && version <= 999 {
						if isFlexible {
							length := uint32(len(structItem.Configs[i].Synonyms) + 1)
							if err := protocol.WriteVaruint32(elemW, length); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteInt32(elemW, int32(len(structItem.Configs[i].Synonyms))); err != nil {
								return nil, err
							}
						}
						for i := range structItem.Configs[i].Synonyms {
							// Name
							if version >= 1 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactString(elemW, structItem.Configs[i].Synonyms[i].Name); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteString(elemW, structItem.Configs[i].Synonyms[i].Name); err != nil {
										return nil, err
									}
								}
							}
							// Value
							if version >= 1 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, structItem.Configs[i].Synonyms[i].Value); err != nil {
										return nil, err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, structItem.Configs[i].Synonyms[i].Value); err != nil {
										return nil, err
									}
								}
							}
							// Source
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, structItem.Configs[i].Synonyms[i].Source); err != nil {
									return nil, err
								}
							}
						}
					}
					// ConfigType
					if version >= 3 && version <= 999 {
						if err := protocol.WriteInt8(elemW, structItem.Configs[i].ConfigType); err != nil {
							return nil, err
						}
					}
					// Documentation
					if version >= 3 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactNullableString(elemW, structItem.Configs[i].Documentation); err != nil {
								return nil, err
							}
						} else {
							if err := protocol.WriteNullableString(elemW, structItem.Configs[i].Documentation); err != nil {
								return nil, err
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
		items := make([]interface{}, len(m.Results))
		for i := range m.Results {
			items[i] = m.Results[i]
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
		// Decode array using ArrayDecoder
		decoder := func(data []byte) (interface{}, int, error) {
			var elem DescribeConfigsResponseDescribeConfigsResult
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
			// ResourceType
			if version >= 0 && version <= 999 {
				val, err := protocol.ReadInt8(elemR)
				if err != nil {
					return nil, 0, err
				}
				elem.ResourceType = val
			}
			// ResourceName
			if version >= 0 && version <= 999 {
				if isFlexible {
					val, err := protocol.ReadCompactString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceName = val
				} else {
					val, err := protocol.ReadString(elemR)
					if err != nil {
						return nil, 0, err
					}
					elem.ResourceName = val
				}
			}
			// Configs
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
				var tempElem DescribeConfigsResponseDescribeConfigsResult
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
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeConfigsResponseDescribeConfigsResourceResult
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
						// Value
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							}
						}
						// ReadOnly
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReadOnly = val
						}
						// ConfigSource
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigSource = val
						}
						// IsSensitive
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsSensitive = val
						}
						// Synonyms
						if version >= 1 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// ConfigType
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigType = val
						}
						// Documentation
						if version >= 3 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Documentation = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Documentation = val
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
							var tempElem DescribeConfigsResponseDescribeConfigsResourceResult
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeConfigsResponseDescribeConfigsSynonym
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
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Source = val
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
									return err
								}
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Synonyms) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Synonyms))); err != nil {
										return err
									}
								}
								for i := range tempElem.Synonyms {
									// Name
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.Synonyms[i].Source); err != nil {
											return err
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigType); err != nil {
									return err
								}
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Documentation); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Documentation); err != nil {
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
						tempElem.Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(DescribeConfigsResponseDescribeConfigsResourceResult)
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
							var tempElem DescribeConfigsResponseDescribeConfigsResourceResult
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeConfigsResponseDescribeConfigsSynonym
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
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Source = val
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
									return err
								}
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Synonyms) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Synonyms))); err != nil {
										return err
									}
								}
								for i := range tempElem.Synonyms {
									// Name
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.Synonyms[i].Source); err != nil {
											return err
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigType); err != nil {
									return err
								}
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Documentation); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Documentation); err != nil {
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
						tempElem.Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(DescribeConfigsResponseDescribeConfigsResourceResult)
						}
					}
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
				// ResourceType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
						return err
					}
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Configs) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs))); err != nil {
							return err
						}
					}
					for i := range tempElem.Configs {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Configs[i].Name); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Value); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Value); err != nil {
									return err
								}
							}
						}
						// ReadOnly
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Configs[i].ReadOnly); err != nil {
								return err
							}
						}
						// ConfigSource
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigSource); err != nil {
								return err
							}
						}
						// IsSensitive
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Configs[i].IsSensitive); err != nil {
								return err
							}
						}
						// Synonyms
						if version >= 1 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Configs[i].Synonyms) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs[i].Synonyms))); err != nil {
									return err
								}
							}
							for i := range tempElem.Configs[i].Synonyms {
								// Name
								if version >= 1 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Synonyms[i].Name); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Configs[i].Synonyms[i].Name); err != nil {
											return err
										}
									}
								}
								// Value
								if version >= 1 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Synonyms[i].Value); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Synonyms[i].Value); err != nil {
											return err
										}
									}
								}
								// Source
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt8(elemW, tempElem.Configs[i].Synonyms[i].Source); err != nil {
										return err
									}
								}
							}
						}
						// ConfigType
						if version >= 3 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigType); err != nil {
								return err
							}
						}
						// Documentation
						if version >= 3 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Documentation); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Documentation); err != nil {
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
			m.Results = make([]DescribeConfigsResponseDescribeConfigsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeConfigsResponseDescribeConfigsResult)
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
				var tempElem DescribeConfigsResponseDescribeConfigsResult
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
				// ResourceType
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt8(r)
					if err != nil {
						return err
					}
					tempElem.ResourceType = val
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						tempElem.ResourceName = val
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					// Decode array using ArrayDecoder
					decoder := func(data []byte) (interface{}, int, error) {
						var elem DescribeConfigsResponseDescribeConfigsResourceResult
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
						// Value
						if version >= 0 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Value = val
							}
						}
						// ReadOnly
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ReadOnly = val
						}
						// ConfigSource
						if version >= 1 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigSource = val
						}
						// IsSensitive
						if version >= 0 && version <= 999 {
							val, err := protocol.ReadBool(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.IsSensitive = val
						}
						// Synonyms
						if version >= 1 && version <= 999 {
							// Nested array in decoder - manual handling needed
							return nil, 0, errors.New("nested arrays in decoder not fully supported")
						}
						// ConfigType
						if version >= 3 && version <= 999 {
							val, err := protocol.ReadInt8(elemR)
							if err != nil {
								return nil, 0, err
							}
							elem.ConfigType = val
						}
						// Documentation
						if version >= 3 && version <= 999 {
							if isFlexible {
								val, err := protocol.ReadCompactNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Documentation = val
							} else {
								val, err := protocol.ReadNullableString(elemR)
								if err != nil {
									return nil, 0, err
								}
								elem.Documentation = val
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
							var tempElem DescribeConfigsResponseDescribeConfigsResourceResult
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeConfigsResponseDescribeConfigsSynonym
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
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Source = val
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
									return err
								}
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Synonyms) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Synonyms))); err != nil {
										return err
									}
								}
								for i := range tempElem.Synonyms {
									// Name
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.Synonyms[i].Source); err != nil {
											return err
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigType); err != nil {
									return err
								}
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Documentation); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Documentation); err != nil {
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
						tempElem.Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(DescribeConfigsResponseDescribeConfigsResourceResult)
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
							var tempElem DescribeConfigsResponseDescribeConfigsResourceResult
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Value = val
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.ReadOnly = val
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigSource = val
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								val, err := protocol.ReadBool(r)
								if err != nil {
									return err
								}
								tempElem.IsSensitive = val
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								// Decode array using ArrayDecoder
								decoder := func(data []byte) (interface{}, int, error) {
									var elem DescribeConfigsResponseDescribeConfigsSynonym
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
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											val, err := protocol.ReadCompactNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										} else {
											val, err := protocol.ReadNullableString(elemR)
											if err != nil {
												return nil, 0, err
											}
											elem.Value = val
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										val, err := protocol.ReadInt8(elemR)
										if err != nil {
											return nil, 0, err
										}
										elem.Source = val
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
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
										var tempElem DescribeConfigsResponseDescribeConfigsSynonym
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												val, err := protocol.ReadCompactNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											} else {
												val, err := protocol.ReadNullableString(r)
												if err != nil {
													return err
												}
												tempElem.Value = val
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											val, err := protocol.ReadInt8(r)
											if err != nil {
												return err
											}
											tempElem.Source = val
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
										// Value
										if version >= 1 && version <= 999 {
											if isFlexible {
												if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											} else {
												if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
													return err
												}
											}
										}
										// Source
										if version >= 1 && version <= 999 {
											if err := protocol.WriteInt8(elemW, tempElem.Source); err != nil {
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
									tempElem.Synonyms = make([]DescribeConfigsResponseDescribeConfigsSynonym, len(decoded))
									for i, item := range decoded {
										tempElem.Synonyms[i] = item.(DescribeConfigsResponseDescribeConfigsSynonym)
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								val, err := protocol.ReadInt8(r)
								if err != nil {
									return err
								}
								tempElem.ConfigType = val
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
								} else {
									val, err := protocol.ReadNullableString(r)
									if err != nil {
										return err
									}
									tempElem.Documentation = val
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
							// Value
							if version >= 0 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Value); err != nil {
										return err
									}
								}
							}
							// ReadOnly
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.ReadOnly); err != nil {
									return err
								}
							}
							// ConfigSource
							if version >= 1 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigSource); err != nil {
									return err
								}
							}
							// IsSensitive
							if version >= 0 && version <= 999 {
								if err := protocol.WriteBool(elemW, tempElem.IsSensitive); err != nil {
									return err
								}
							}
							// Synonyms
							if version >= 1 && version <= 999 {
								if isFlexible {
									length := uint32(len(tempElem.Synonyms) + 1)
									if err := protocol.WriteVaruint32(elemW, length); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteInt32(elemW, int32(len(tempElem.Synonyms))); err != nil {
										return err
									}
								}
								for i := range tempElem.Synonyms {
									// Name
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteString(elemW, tempElem.Synonyms[i].Name); err != nil {
												return err
											}
										}
									}
									// Value
									if version >= 1 && version <= 999 {
										if isFlexible {
											if err := protocol.WriteCompactNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										} else {
											if err := protocol.WriteNullableString(elemW, tempElem.Synonyms[i].Value); err != nil {
												return err
											}
										}
									}
									// Source
									if version >= 1 && version <= 999 {
										if err := protocol.WriteInt8(elemW, tempElem.Synonyms[i].Source); err != nil {
											return err
										}
									}
								}
							}
							// ConfigType
							if version >= 3 && version <= 999 {
								if err := protocol.WriteInt8(elemW, tempElem.ConfigType); err != nil {
									return err
								}
							}
							// Documentation
							if version >= 3 && version <= 999 {
								if isFlexible {
									if err := protocol.WriteCompactNullableString(elemW, tempElem.Documentation); err != nil {
										return err
									}
								} else {
									if err := protocol.WriteNullableString(elemW, tempElem.Documentation); err != nil {
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
						tempElem.Configs = make([]DescribeConfigsResponseDescribeConfigsResourceResult, len(decoded))
						for i, item := range decoded {
							tempElem.Configs[i] = item.(DescribeConfigsResponseDescribeConfigsResourceResult)
						}
					}
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
				// ResourceType
				if version >= 0 && version <= 999 {
					if err := protocol.WriteInt8(elemW, tempElem.ResourceType); err != nil {
						return err
					}
				}
				// ResourceName
				if version >= 0 && version <= 999 {
					if isFlexible {
						if err := protocol.WriteCompactString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteString(elemW, tempElem.ResourceName); err != nil {
							return err
						}
					}
				}
				// Configs
				if version >= 0 && version <= 999 {
					if isFlexible {
						length := uint32(len(tempElem.Configs) + 1)
						if err := protocol.WriteVaruint32(elemW, length); err != nil {
							return err
						}
					} else {
						if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs))); err != nil {
							return err
						}
					}
					for i := range tempElem.Configs {
						// Name
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Name); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteString(elemW, tempElem.Configs[i].Name); err != nil {
									return err
								}
							}
						}
						// Value
						if version >= 0 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Value); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Value); err != nil {
									return err
								}
							}
						}
						// ReadOnly
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Configs[i].ReadOnly); err != nil {
								return err
							}
						}
						// ConfigSource
						if version >= 1 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigSource); err != nil {
								return err
							}
						}
						// IsSensitive
						if version >= 0 && version <= 999 {
							if err := protocol.WriteBool(elemW, tempElem.Configs[i].IsSensitive); err != nil {
								return err
							}
						}
						// Synonyms
						if version >= 1 && version <= 999 {
							if isFlexible {
								length := uint32(len(tempElem.Configs[i].Synonyms) + 1)
								if err := protocol.WriteVaruint32(elemW, length); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteInt32(elemW, int32(len(tempElem.Configs[i].Synonyms))); err != nil {
									return err
								}
							}
							for i := range tempElem.Configs[i].Synonyms {
								// Name
								if version >= 1 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactString(elemW, tempElem.Configs[i].Synonyms[i].Name); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteString(elemW, tempElem.Configs[i].Synonyms[i].Name); err != nil {
											return err
										}
									}
								}
								// Value
								if version >= 1 && version <= 999 {
									if isFlexible {
										if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Synonyms[i].Value); err != nil {
											return err
										}
									} else {
										if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Synonyms[i].Value); err != nil {
											return err
										}
									}
								}
								// Source
								if version >= 1 && version <= 999 {
									if err := protocol.WriteInt8(elemW, tempElem.Configs[i].Synonyms[i].Source); err != nil {
										return err
									}
								}
							}
						}
						// ConfigType
						if version >= 3 && version <= 999 {
							if err := protocol.WriteInt8(elemW, tempElem.Configs[i].ConfigType); err != nil {
								return err
							}
						}
						// Documentation
						if version >= 3 && version <= 999 {
							if isFlexible {
								if err := protocol.WriteCompactNullableString(elemW, tempElem.Configs[i].Documentation); err != nil {
									return err
								}
							} else {
								if err := protocol.WriteNullableString(elemW, tempElem.Configs[i].Documentation); err != nil {
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
			m.Results = make([]DescribeConfigsResponseDescribeConfigsResult, len(decoded))
			for i, item := range decoded {
				m.Results[i] = item.(DescribeConfigsResponseDescribeConfigsResult)
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsResponseDescribeConfigsResult.
func (m *DescribeConfigsResponseDescribeConfigsResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsResponseDescribeConfigsResult.
func (m *DescribeConfigsResponseDescribeConfigsResult) readTaggedFields(r io.Reader, version int16) error {
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
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsResponseDescribeConfigsResourceResult.
func (m *DescribeConfigsResponseDescribeConfigsResourceResult) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsResponseDescribeConfigsResourceResult.
func (m *DescribeConfigsResponseDescribeConfigsResourceResult) readTaggedFields(r io.Reader, version int16) error {
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

// DescribeConfigsResponseDescribeConfigsSynonym represents The synonyms for this configuration key..
type DescribeConfigsResponseDescribeConfigsSynonym struct {
	// The synonym name.
	Name string `json:"name" versions:"1-999"`
	// The synonym value.
	Value *string `json:"value" versions:"1-999"`
	// The synonym source.
	Source int8 `json:"source" versions:"1-999"`
	// Tagged fields (for flexible versions)
	_tagged_fields map[uint32]interface{} `json:"-"`
}

// writeTaggedFields writes tagged fields for DescribeConfigsResponseDescribeConfigsSynonym.
func (m *DescribeConfigsResponseDescribeConfigsSynonym) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeConfigsResponseDescribeConfigsSynonym.
func (m *DescribeConfigsResponseDescribeConfigsSynonym) readTaggedFields(r io.Reader, version int16) error {
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
