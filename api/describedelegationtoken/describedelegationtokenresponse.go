package describedelegationtoken

import (
	"bytes"
	"errors"
	"io"

	"github.com/scholzj/go-kafka-protocol/protocol"
)

const (
	DescribeDelegationTokenResponseApiKey        = 41
	DescribeDelegationTokenResponseHeaderVersion = 1
)

// DescribeDelegationTokenResponse represents a response message.
type DescribeDelegationTokenResponse struct {
	// The error code, or 0 if there was no error.
	ErrorCode int16 `json:"errorcode" versions:"0-999"`
	// The tokens.
	Tokens []DescribeDelegationTokenResponseDescribedDelegationToken `json:"tokens" versions:"0-999"`
	// The duration in milliseconds for which the request was throttled due to a quota violation, or zero if the request did not violate any quota.
	ThrottleTimeMs int32 `json:"throttletimems" versions:"0-999"`
}

// Encode encodes a DescribeDelegationTokenResponse to a byte slice for the given version.
func (m *DescribeDelegationTokenResponse) Encode(version int16) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Write(&buf, version); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes a DescribeDelegationTokenResponse from a byte slice for the given version.
func (m *DescribeDelegationTokenResponse) Decode(data []byte, version int16) error {
	r := bytes.NewReader(data)
	return m.Read(r, version)
}

// Write writes a DescribeDelegationTokenResponse to an io.Writer for the given version.
func (m *DescribeDelegationTokenResponse) Write(w io.Writer, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt16(w, m.ErrorCode); err != nil {
			return err
		}
	}
	// Tokens
	if version >= 0 && version <= 999 {
		if isFlexible {
			length := uint32(len(m.Tokens) + 1)
			if err := protocol.WriteVaruint32(w, length); err != nil {
				return err
			}
		} else {
			if err := protocol.WriteInt32(w, int32(len(m.Tokens))); err != nil {
				return err
			}
		}
		for i := range m.Tokens {
			// PrincipalType
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Tokens[i].PrincipalType); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Tokens[i].PrincipalType); err != nil {
						return err
					}
				}
			}
			// PrincipalName
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Tokens[i].PrincipalName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Tokens[i].PrincipalName); err != nil {
						return err
					}
				}
			}
			// TokenRequesterPrincipalType
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Tokens[i].TokenRequesterPrincipalType); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Tokens[i].TokenRequesterPrincipalType); err != nil {
						return err
					}
				}
			}
			// TokenRequesterPrincipalName
			if version >= 3 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Tokens[i].TokenRequesterPrincipalName); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Tokens[i].TokenRequesterPrincipalName); err != nil {
						return err
					}
				}
			}
			// IssueTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Tokens[i].IssueTimestamp); err != nil {
					return err
				}
			}
			// ExpiryTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Tokens[i].ExpiryTimestamp); err != nil {
					return err
				}
			}
			// MaxTimestamp
			if version >= 0 && version <= 999 {
				if err := protocol.WriteInt64(w, m.Tokens[i].MaxTimestamp); err != nil {
					return err
				}
			}
			// TokenId
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactString(w, m.Tokens[i].TokenId); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteString(w, m.Tokens[i].TokenId); err != nil {
						return err
					}
				}
			}
			// Hmac
			if version >= 0 && version <= 999 {
				if isFlexible {
					if err := protocol.WriteCompactBytes(w, m.Tokens[i].Hmac); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteBytes(w, m.Tokens[i].Hmac); err != nil {
						return err
					}
				}
			}
			// Renewers
			if version >= 0 && version <= 999 {
				if isFlexible {
					length := uint32(len(m.Tokens[i].Renewers) + 1)
					if err := protocol.WriteVaruint32(w, length); err != nil {
						return err
					}
				} else {
					if err := protocol.WriteInt32(w, int32(len(m.Tokens[i].Renewers))); err != nil {
						return err
					}
				}
				for i := range m.Tokens[i].Renewers {
					// PrincipalType
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Tokens[i].Renewers[i].PrincipalType); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Tokens[i].Renewers[i].PrincipalType); err != nil {
								return err
							}
						}
					}
					// PrincipalName
					if version >= 0 && version <= 999 {
						if isFlexible {
							if err := protocol.WriteCompactString(w, m.Tokens[i].Renewers[i].PrincipalName); err != nil {
								return err
							}
						} else {
							if err := protocol.WriteString(w, m.Tokens[i].Renewers[i].PrincipalName); err != nil {
								return err
							}
						}
					}
				}
			}
		}
	}
	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		if err := protocol.WriteInt32(w, m.ThrottleTimeMs); err != nil {
			return err
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

// Read reads a DescribeDelegationTokenResponse from an io.Reader for the given version.
func (m *DescribeDelegationTokenResponse) Read(r io.Reader, version int16) error {
	if version < 1 || version > 3 {
		return errors.New("unsupported version")
	}

	isFlexible := false
	if version >= 2 {
		isFlexible = true
	}

	// ErrorCode
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt16(r)
		if err != nil {
			return err
		}
		m.ErrorCode = val
	}
	// Tokens
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
			m.Tokens = make([]DescribeDelegationTokenResponseDescribedDelegationToken, length)
			for i := int32(0); i < length; i++ {
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalName = val
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalType = val
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalName = val
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].IssueTimestamp = val
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].ExpiryTimestamp = val
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].MaxTimestamp = val
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenId = val
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Hmac = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Hmac = val
					}
				}
				// Renewers
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
						m.Tokens[i].Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, length)
						for i := int32(0); i < length; i++ {
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, length)
						for i := int32(0); i < length; i++ {
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
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
			m.Tokens = make([]DescribeDelegationTokenResponseDescribedDelegationToken, length)
			for i := int32(0); i < length; i++ {
				// PrincipalType
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalType = val
					}
				}
				// PrincipalName
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].PrincipalName = val
					}
				}
				// TokenRequesterPrincipalType
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalType = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalType = val
					}
				}
				// TokenRequesterPrincipalName
				if version >= 3 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalName = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenRequesterPrincipalName = val
					}
				}
				// IssueTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].IssueTimestamp = val
				}
				// ExpiryTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].ExpiryTimestamp = val
				}
				// MaxTimestamp
				if version >= 0 && version <= 999 {
					val, err := protocol.ReadInt64(r)
					if err != nil {
						return err
					}
					m.Tokens[i].MaxTimestamp = val
				}
				// TokenId
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenId = val
					} else {
						val, err := protocol.ReadString(r)
						if err != nil {
							return err
						}
						m.Tokens[i].TokenId = val
					}
				}
				// Hmac
				if version >= 0 && version <= 999 {
					if isFlexible {
						val, err := protocol.ReadCompactBytes(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Hmac = val
					} else {
						val, err := protocol.ReadBytes(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Hmac = val
					}
				}
				// Renewers
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
						m.Tokens[i].Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, length)
						for i := int32(0); i < length; i++ {
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								}
							}
						}
					} else {
						var err error
						length, err = protocol.ReadInt32(r)
						if err != nil {
							return err
						}
						m.Tokens[i].Renewers = make([]DescribeDelegationTokenResponseDescribedDelegationTokenRenewer, length)
						for i := int32(0); i < length; i++ {
							// PrincipalType
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalType = val
								}
							}
							// PrincipalName
							if version >= 0 && version <= 999 {
								if isFlexible {
									val, err := protocol.ReadCompactString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								} else {
									val, err := protocol.ReadString(r)
									if err != nil {
										return err
									}
									m.Tokens[i].Renewers[i].PrincipalName = val
								}
							}
						}
					}
				}
			}
		}
	}
	// ThrottleTimeMs
	if version >= 0 && version <= 999 {
		val, err := protocol.ReadInt32(r)
		if err != nil {
			return err
		}
		m.ThrottleTimeMs = val
	}
	// Read tagged fields if flexible
	if isFlexible {
		if err := m.readTaggedFields(r, version); err != nil {
			return err
		}
	}
	return nil
}

// DescribeDelegationTokenResponseDescribedDelegationToken represents The tokens..
type DescribeDelegationTokenResponseDescribedDelegationToken struct {
	// The token principal type.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The token principal name.
	PrincipalName string `json:"principalname" versions:"0-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalType string `json:"tokenrequesterprincipaltype" versions:"3-999"`
	// The principal type of the requester of the token.
	TokenRequesterPrincipalName string `json:"tokenrequesterprincipalname" versions:"3-999"`
	// The token issue timestamp in milliseconds.
	IssueTimestamp int64 `json:"issuetimestamp" versions:"0-999"`
	// The token expiry timestamp in milliseconds.
	ExpiryTimestamp int64 `json:"expirytimestamp" versions:"0-999"`
	// The token maximum timestamp length in milliseconds.
	MaxTimestamp int64 `json:"maxtimestamp" versions:"0-999"`
	// The token ID.
	TokenId string `json:"tokenid" versions:"0-999"`
	// The token HMAC.
	Hmac []byte `json:"hmac" versions:"0-999"`
	// Those who are able to renew this token before it expires.
	Renewers []DescribeDelegationTokenResponseDescribedDelegationTokenRenewer `json:"renewers" versions:"0-999"`
}

// DescribeDelegationTokenResponseDescribedDelegationTokenRenewer represents Those who are able to renew this token before it expires..
type DescribeDelegationTokenResponseDescribedDelegationTokenRenewer struct {
	// The renewer principal type.
	PrincipalType string `json:"principaltype" versions:"0-999"`
	// The renewer principal name.
	PrincipalName string `json:"principalname" versions:"0-999"`
}

// writeTaggedFields writes tagged fields for DescribeDelegationTokenResponse.
func (m *DescribeDelegationTokenResponse) writeTaggedFields(w io.Writer, version int16) error {
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

// readTaggedFields reads tagged fields for DescribeDelegationTokenResponse.
func (m *DescribeDelegationTokenResponse) readTaggedFields(r io.Reader, version int16) error {
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
