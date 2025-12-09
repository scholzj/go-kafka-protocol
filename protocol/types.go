package protocol

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"github.com/google/uuid"
)

// Errors
var (
	ErrInsufficientData = errors.New("insufficient data to decode")
	ErrInvalidVarint    = errors.New("invalid varint encoding")
	ErrInvalidVarlong   = errors.New("invalid varlong encoding")
)

// ============================================================================
// INT8 - 8-bit signed integer
// ============================================================================

// EncodeInt8 encodes an int8 to a byte slice.
func EncodeInt8(value int8) []byte {
	return []byte{byte(value)}
}

// DecodeInt8 decodes an int8 from a byte slice.
func DecodeInt8(data []byte) (int8, error) {
	if len(data) < 1 {
		return 0, ErrInsufficientData
	}
	return int8(data[0]), nil
}

// WriteInt8 writes an int8 to an io.Writer.
func WriteInt8(w io.Writer, value int8) error {
	_, err := w.Write(EncodeInt8(value))
	return err
}

// ReadInt8 reads an int8 from an io.Reader.
func ReadInt8(r io.Reader) (int8, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeInt8(buf)
}

// ============================================================================
// BOOLEAN - boolean value (encoded as single byte: 0 for false, 1 for true)
// ============================================================================

// EncodeBool encodes a bool to a byte slice.
// In Kafka protocol, boolean is encoded as a single byte: 0 for false, 1 for true.
func EncodeBool(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

// DecodeBool decodes a bool from a byte slice.
// In Kafka protocol, boolean is encoded as a single byte: 0 for false, 1 for true.
func DecodeBool(data []byte) (bool, error) {
	if len(data) < 1 {
		return false, ErrInsufficientData
	}
	return data[0] != 0, nil
}

// WriteBool writes a bool to an io.Writer.
func WriteBool(w io.Writer, value bool) error {
	_, err := w.Write(EncodeBool(value))
	return err
}

// ReadBool reads a bool from an io.Reader.
func ReadBool(r io.Reader) (bool, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(r, buf); err != nil {
		return false, err
	}
	return DecodeBool(buf)
}

// ============================================================================
// INT16 - 16-bit signed integer (big-endian)
// ============================================================================

// EncodeInt16 encodes an int16 to a byte slice in big-endian order.
func EncodeInt16(value int16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(value))
	return buf
}

// DecodeInt16 decodes an int16 from a byte slice in big-endian order.
func DecodeInt16(data []byte) (int16, error) {
	if len(data) < 2 {
		return 0, ErrInsufficientData
	}
	return int16(binary.BigEndian.Uint16(data[:2])), nil
}

// WriteInt16 writes an int16 to an io.Writer.
func WriteInt16(w io.Writer, value int16) error {
	_, err := w.Write(EncodeInt16(value))
	return err
}

// ReadInt16 reads an int16 from an io.Reader.
func ReadInt16(r io.Reader) (int16, error) {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeInt16(buf)
}

// ============================================================================
// UINT16 - 16-bit unsigned integer (big-endian)
// ============================================================================

// EncodeUint16 encodes a uint16 to a byte slice in big-endian order.
func EncodeUint16(value uint16) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, value)
	return buf
}

// DecodeUint16 decodes a uint16 from a byte slice in big-endian order.
func DecodeUint16(data []byte) (uint16, error) {
	if len(data) < 2 {
		return 0, ErrInsufficientData
	}
	return binary.BigEndian.Uint16(data[:2]), nil
}

// WriteUint16 writes a uint16 to an io.Writer.
func WriteUint16(w io.Writer, value uint16) error {
	_, err := w.Write(EncodeUint16(value))
	return err
}

// ReadUint16 reads a uint16 from an io.Reader.
func ReadUint16(r io.Reader) (uint16, error) {
	buf := make([]byte, 2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeUint16(buf)
}

// ============================================================================
// INT32 - 32-bit signed integer (big-endian)
// ============================================================================

// EncodeInt32 encodes an int32 to a byte slice in big-endian order.
func EncodeInt32(value int32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return buf
}

// DecodeInt32 decodes an int32 from a byte slice in big-endian order.
func DecodeInt32(data []byte) (int32, error) {
	if len(data) < 4 {
		return 0, ErrInsufficientData
	}
	return int32(binary.BigEndian.Uint32(data[:4])), nil
}

// WriteInt32 writes an int32 to an io.Writer.
func WriteInt32(w io.Writer, value int32) error {
	_, err := w.Write(EncodeInt32(value))
	return err
}

// ReadInt32 reads an int32 from an io.Reader.
func ReadInt32(r io.Reader) (int32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeInt32(buf)
}

// ============================================================================
// UINT32 - 32-bit unsigned integer (big-endian)
// ============================================================================

// EncodeUint32 encodes a uint32 to a byte slice in big-endian order.
func EncodeUint32(value uint32) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, value)
	return buf
}

// DecodeUint32 decodes a uint32 from a byte slice in big-endian order.
func DecodeUint32(data []byte) (uint32, error) {
	if len(data) < 4 {
		return 0, ErrInsufficientData
	}
	return binary.BigEndian.Uint32(data[:4]), nil
}

// WriteUint32 writes a uint32 to an io.Writer.
func WriteUint32(w io.Writer, value uint32) error {
	_, err := w.Write(EncodeUint32(value))
	return err
}

// ReadUint32 reads a uint32 from an io.Reader.
func ReadUint32(r io.Reader) (uint32, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeUint32(buf)
}

// ============================================================================
// INT64 - 64-bit signed integer (big-endian)
// ============================================================================

// EncodeInt64 encodes an int64 to a byte slice in big-endian order.
func EncodeInt64(value int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(value))
	return buf
}

// DecodeInt64 decodes an int64 from a byte slice in big-endian order.
func DecodeInt64(data []byte) (int64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	return int64(binary.BigEndian.Uint64(data[:8])), nil
}

// WriteInt64 writes an int64 to an io.Writer.
func WriteInt64(w io.Writer, value int64) error {
	_, err := w.Write(EncodeInt64(value))
	return err
}

// ReadInt64 reads an int64 from an io.Reader.
func ReadInt64(r io.Reader) (int64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeInt64(buf)
}

// ============================================================================
// FLOAT64 - 64-bit IEEE 754 floating point number (big-endian)
// ============================================================================

// EncodeFloat64 encodes a float64 to a byte slice in big-endian order.
func EncodeFloat64(value float64) []byte {
	buf := make([]byte, 8)
	bits := math.Float64bits(value)
	binary.BigEndian.PutUint64(buf, bits)
	return buf
}

// DecodeFloat64 decodes a float64 from a byte slice in big-endian order.
func DecodeFloat64(data []byte) (float64, error) {
	if len(data) < 8 {
		return 0, ErrInsufficientData
	}
	bits := binary.BigEndian.Uint64(data[:8])
	return math.Float64frombits(bits), nil
}

// WriteFloat64 writes a float64 to an io.Writer.
func WriteFloat64(w io.Writer, value float64) error {
	_, err := w.Write(EncodeFloat64(value))
	return err
}

// ReadFloat64 reads a float64 from an io.Reader.
func ReadFloat64(r io.Reader) (float64, error) {
	buf := make([]byte, 8)
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	}
	return DecodeFloat64(buf)
}

// ============================================================================
// STRING - UTF-8 string prefixed with a 16-bit length (big-endian)
// ============================================================================

// EncodeString encodes a string to a byte slice with a 2-byte length prefix.
func EncodeString(value string) []byte {
	strBytes := []byte(value)
	length := int16(len(strBytes))
	buf := make([]byte, 2+len(strBytes))
	binary.BigEndian.PutUint16(buf, uint16(length))
	copy(buf[2:], strBytes)
	return buf
}

// DecodeString decodes a string from a byte slice with a 2-byte length prefix.
func DecodeString(data []byte) (string, int, error) {
	if len(data) < 2 {
		return "", 0, ErrInsufficientData
	}
	length := int(binary.BigEndian.Uint16(data[:2]))
	if len(data) < 2+length {
		return "", 0, ErrInsufficientData
	}
	return string(data[2 : 2+length]), 2 + length, nil
}

// WriteString writes a string to an io.Writer.
func WriteString(w io.Writer, value string) error {
	_, err := w.Write(EncodeString(value))
	return err
}

// ReadString reads a string from an io.Reader.
func ReadString(r io.Reader) (string, error) {
	lengthBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return "", err
	}
	length := int(binary.BigEndian.Uint16(lengthBuf))
	if length < 0 {
		return "", errors.New("invalid string length")
	}
	strBuf := make([]byte, length)
	if _, err := io.ReadFull(r, strBuf); err != nil {
		return "", err
	}
	return string(strBuf), nil
}

// ============================================================================
// NULLABLE_STRING - UTF-8 string or null, prefixed with a 16-bit length (-1 for null)
// ============================================================================

// EncodeNullableString encodes a nullable string to a byte slice.
// If value is nil, encodes -1 as the length.
func EncodeNullableString(value *string) []byte {
	if value == nil {
		return EncodeInt16(-1)
	}
	return EncodeString(*value)
}

// DecodeNullableString decodes a nullable string from a byte slice.
func DecodeNullableString(data []byte) (*string, int, error) {
	if len(data) < 2 {
		return nil, 0, ErrInsufficientData
	}
	length := int16(binary.BigEndian.Uint16(data[:2]))
	if length == -1 {
		return nil, 2, nil
	}
	str, n, err := DecodeString(data)
	if err != nil {
		return nil, 0, err
	}
	return &str, n, nil
}

// WriteNullableString writes a nullable string to an io.Writer.
func WriteNullableString(w io.Writer, value *string) error {
	_, err := w.Write(EncodeNullableString(value))
	return err
}

// ReadNullableString reads a nullable string from an io.Reader.
func ReadNullableString(r io.Reader) (*string, error) {
	lengthBuf := make([]byte, 2)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := int16(binary.BigEndian.Uint16(lengthBuf))
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid string length")
	}
	strBuf := make([]byte, length)
	if _, err := io.ReadFull(r, strBuf); err != nil {
		return nil, err
	}
	str := string(strBuf)
	return &str, nil
}

// ============================================================================
// BYTES - Byte array prefixed with a 32-bit length (big-endian)
// ============================================================================

// EncodeBytes encodes a byte slice with a 4-byte length prefix.
func EncodeBytes(value []byte) []byte {
	if value == nil {
		return EncodeInt32(-1)
	}
	length := int32(len(value))
	buf := make([]byte, 4+len(value))
	binary.BigEndian.PutUint32(buf, uint32(length))
	copy(buf[4:], value)
	return buf
}

// DecodeBytes decodes a byte slice with a 4-byte length prefix.
func DecodeBytes(data []byte) ([]byte, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length := int32(binary.BigEndian.Uint32(data[:4]))
	if length < 0 {
		return nil, 4, nil
	}
	if len(data) < 4+int(length) {
		return nil, 0, ErrInsufficientData
	}
	result := make([]byte, length)
	copy(result, data[4:4+length])
	return result, 4 + int(length), nil
}

// WriteBytes writes a byte slice to an io.Writer.
func WriteBytes(w io.Writer, value []byte) error {
	_, err := w.Write(EncodeBytes(value))
	return err
}

// ReadBytes reads a byte slice from an io.Reader.
func ReadBytes(r io.Reader) ([]byte, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(lengthBuf))
	if length < 0 {
		return nil, nil
	}
	if length == 0 {
		return []byte{}, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ============================================================================
// NULLABLE_BYTES - Byte array or null, prefixed with a 32-bit length (-1 for null)
// ============================================================================

// EncodeNullableBytes encodes a nullable byte slice to a byte slice.
// If value is nil, encodes -1 as the length.
func EncodeNullableBytes(value *[]byte) []byte {
	if value == nil {
		return EncodeInt32(-1)
	}
	return EncodeBytes(*value)
}

// DecodeNullableBytes decodes a nullable byte slice from a byte slice.
func DecodeNullableBytes(data []byte) (*[]byte, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length := int32(binary.BigEndian.Uint32(data[:4]))
	if length == -1 {
		return nil, 4, nil
	}
	bytes, n, err := DecodeBytes(data)
	if err != nil {
		return nil, 0, err
	}
	return &bytes, n, nil
}

// WriteNullableBytes writes a nullable byte slice to an io.Writer.
func WriteNullableBytes(w io.Writer, value *[]byte) error {
	_, err := w.Write(EncodeNullableBytes(value))
	return err
}

// ReadNullableBytes reads a nullable byte slice from an io.Reader.
func ReadNullableBytes(r io.Reader) (*[]byte, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length := int32(binary.BigEndian.Uint32(lengthBuf))
	if length == -1 {
		return nil, nil
	}
	if length == 0 {
		empty := []byte{}
		return &empty, nil
	}
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return &data, nil
}

// ============================================================================
// VARINT - ZigZag-encoded variable-length integer
// ============================================================================

// EncodeVarint encodes an int32 as a VARINT using ZigZag encoding.
func EncodeVarint(value int32) []byte {
	// ZigZag encoding: (n << 1) ^ (n >> 31)
	zigzag := uint32((value << 1) ^ (value >> 31))
	return encodeVaruint32(zigzag)
}

// encodeVaruint32 encodes an unsigned 32-bit integer as a variable-length integer.
func encodeVaruint32(value uint32) []byte {
	var buf []byte
	for {
		b := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if value == 0 {
			break
		}
	}
	return buf
}

// DecodeVarint decodes a VARINT from a byte slice, returning the value and bytes consumed.
func DecodeVarint(data []byte) (int32, int, error) {
	zigzag, n, err := decodeVaruint32(data)
	if err != nil {
		return 0, 0, err
	}
	// ZigZag decoding: (zigzag >> 1) ^ -(zigzag & 1)
	value := int32((zigzag >> 1) ^ -(zigzag & 1))
	return value, n, nil
}

// decodeVaruint32 decodes an unsigned variable-length integer from a byte slice.
func decodeVaruint32(data []byte) (uint32, int, error) {
	var result uint32
	var shift uint
	var n int

	for n < len(data) {
		b := data[n]
		n++
		result |= uint32(b&0x7F) << shift
		if (b & 0x80) == 0 {
			return result, n, nil
		}
		shift += 7
		if shift >= 32 {
			return 0, 0, ErrInvalidVarint
		}
	}
	return 0, 0, ErrInsufficientData
}

// WriteVarint writes a VARINT to an io.Writer.
func WriteVarint(w io.Writer, value int32) error {
	_, err := w.Write(EncodeVarint(value))
	return err
}

// ReadVarint reads a VARINT from an io.Reader.
func ReadVarint(r io.Reader) (int32, error) {
	var result uint32
	var shift uint

	for {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		result |= uint32(b&0x7F) << shift
		if (b & 0x80) == 0 {
			// ZigZag decoding: (zigzag >> 1) ^ -(zigzag & 1)
			zigzag := uint32(result)
			value := int32((zigzag >> 1) ^ -(zigzag & 1))
			return value, nil
		}
		shift += 7
		if shift >= 32 {
			return 0, ErrInvalidVarint
		}
	}
}

// ============================================================================
// VARLONG - ZigZag-encoded variable-length long integer
// ============================================================================

// EncodeVarlong encodes an int64 as a VARLONG using ZigZag encoding.
func EncodeVarlong(value int64) []byte {
	// ZigZag encoding: (n << 1) ^ (n >> 63)
	zigzag := uint64((value << 1) ^ (value >> 63))
	return encodeVaruint64(zigzag)
}

// encodeVaruint64 encodes an unsigned 64-bit integer as a variable-length integer.
func encodeVaruint64(value uint64) []byte {
	var buf []byte
	for {
		b := byte(value & 0x7F)
		value >>= 7
		if value != 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if value == 0 {
			break
		}
	}
	return buf
}

// DecodeVarlong decodes a VARLONG from a byte slice, returning the value and bytes consumed.
func DecodeVarlong(data []byte) (int64, int, error) {
	zigzag, n, err := decodeVaruint64(data)
	if err != nil {
		return 0, 0, err
	}
	// ZigZag decoding: (zigzag >> 1) ^ -(zigzag & 1)
	value := int64((zigzag >> 1) ^ -(zigzag & 1))
	return value, n, nil
}

// decodeVaruint64 decodes an unsigned variable-length integer from a byte slice.
func decodeVaruint64(data []byte) (uint64, int, error) {
	var result uint64
	var shift uint
	var n int

	for n < len(data) {
		b := data[n]
		n++
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			return result, n, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, 0, ErrInvalidVarlong
		}
	}
	return 0, 0, ErrInsufficientData
}

// WriteVarlong writes a VARLONG to an io.Writer.
func WriteVarlong(w io.Writer, value int64) error {
	_, err := w.Write(EncodeVarlong(value))
	return err
}

// ReadVarlong reads a VARLONG from an io.Reader.
func ReadVarlong(r io.Reader) (int64, error) {
	var result uint64
	var shift uint

	for {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		result |= uint64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			// ZigZag decoding: (zigzag >> 1) ^ -(zigzag & 1)
			zigzag := uint64(result)
			value := int64((zigzag >> 1) ^ -(zigzag & 1))
			return value, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, ErrInvalidVarlong
		}
	}
}

// ============================================================================
// COMPACT_STRING - UTF-8 string prefixed with an unsigned VARINT length
// ============================================================================

// EncodeCompactString encodes a string with a VARINT length prefix.
func EncodeCompactString(value string) []byte {
	strBytes := []byte(value)
	length := uint32(len(strBytes) + 1) // +1 for the length itself
	buf := encodeVaruint32(length)
	buf = append(buf, strBytes...)
	return buf
}

// DecodeCompactString decodes a compact string from a byte slice.
func DecodeCompactString(data []byte) (string, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return "", 0, err
	}
	if length < 1 {
		return "", 0, errors.New("invalid compact string length")
	}
	length-- // Subtract 1 for the length field itself
	if len(data) < n+int(length) {
		return "", 0, ErrInsufficientData
	}
	return string(data[n : n+int(length)]), n + int(length), nil
}

// WriteCompactString writes a compact string to an io.Writer.
func WriteCompactString(w io.Writer, value string) error {
	_, err := w.Write(EncodeCompactString(value))
	return err
}

// ReadCompactString reads a compact string from an io.Reader.
func ReadCompactString(r io.Reader) (string, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return "", err
	}
	if length < 1 {
		return "", errors.New("invalid compact string length")
	}
	length-- // Subtract 1 for the length field itself
	strBuf := make([]byte, length)
	if _, err := io.ReadFull(r, strBuf); err != nil {
		return "", err
	}
	return string(strBuf), nil
}

// EncodeVaruint32 encodes an unsigned 32-bit integer as a variable-length integer.
func EncodeVaruint32(value uint32) []byte {
	return encodeVaruint32(value)
}

// WriteVaruint32 writes an unsigned VARINT to an io.Writer.
func WriteVaruint32(w io.Writer, value uint32) error {
	_, err := w.Write(EncodeVaruint32(value))
	return err
}

// ReadVaruint32 reads an unsigned VARINT from an io.Reader.
func ReadVaruint32(r io.Reader) (uint32, error) {
	var result uint32
	var shift uint

	for {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		result |= uint32(b&0x7F) << shift
		if (b & 0x80) == 0 {
			return result, nil
		}
		shift += 7
		if shift >= 32 {
			return 0, ErrInvalidVarint
		}
	}
}

// ============================================================================
// COMPACT_NULLABLE_STRING - UTF-8 string or null, prefixed with an unsigned VARINT length (0 for null)
// ============================================================================

// EncodeCompactNullableString encodes a nullable string with a VARINT length prefix.
// If value is nil, encodes 0 as the length.
func EncodeCompactNullableString(value *string) []byte {
	if value == nil {
		return encodeVaruint32(0)
	}
	return EncodeCompactString(*value)
}

// DecodeCompactNullableString decodes a nullable compact string from a byte slice.
func DecodeCompactNullableString(data []byte) (*string, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	length-- // Subtract 1 for the length field itself
	if len(data) < n+int(length) {
		return nil, 0, ErrInsufficientData
	}
	str := string(data[n : n+int(length)])
	return &str, n + int(length), nil
}

// WriteCompactNullableString writes a nullable compact string to an io.Writer.
func WriteCompactNullableString(w io.Writer, value *string) error {
	_, err := w.Write(EncodeCompactNullableString(value))
	return err
}

// ReadCompactNullableString reads a nullable compact string from an io.Reader.
func ReadCompactNullableString(r io.Reader) (*string, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	length-- // Subtract 1 for the length field itself
	strBuf := make([]byte, length)
	if _, err := io.ReadFull(r, strBuf); err != nil {
		return nil, err
	}
	str := string(strBuf)
	return &str, nil
}

// ============================================================================
// COMPACT_BYTES - Byte array prefixed with an unsigned VARINT length
// ============================================================================

// EncodeCompactBytes encodes a byte slice with a VARINT length prefix.
func EncodeCompactBytes(value []byte) []byte {
	if value == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(value) + 1) // +1 for the length itself
	buf := encodeVaruint32(length)
	buf = append(buf, value...)
	return buf
}

// DecodeCompactBytes decodes a compact byte slice from a byte slice.
func DecodeCompactBytes(data []byte) ([]byte, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	length-- // Subtract 1 for the length field itself
	if len(data) < n+int(length) {
		return nil, 0, ErrInsufficientData
	}
	result := make([]byte, length)
	copy(result, data[n:n+int(length)])
	return result, n + int(length), nil
}

// WriteCompactBytes writes a compact byte slice to an io.Writer.
func WriteCompactBytes(w io.Writer, value []byte) error {
	_, err := w.Write(EncodeCompactBytes(value))
	return err
}

// ReadCompactBytes reads a compact byte slice from an io.Reader.
func ReadCompactBytes(r io.Reader) ([]byte, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	length-- // Subtract 1 for the length field itself
	data := make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ============================================================================
// COMPACT_NULLABLE_BYTES - Byte array or null, prefixed with an unsigned VARINT length (0 for null)
// ============================================================================

// EncodeCompactNullableBytes encodes a nullable byte slice with a VARINT length prefix.
// If value is nil, encodes 0 as the length.
func EncodeCompactNullableBytes(value *[]byte) []byte {
	if value == nil {
		return encodeVaruint32(0)
	}
	return EncodeCompactBytes(*value)
}

// DecodeCompactNullableBytes decodes a nullable compact byte slice from a byte slice.
func DecodeCompactNullableBytes(data []byte) (*[]byte, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	bytes, consumed, err := DecodeCompactBytes(data)
	if err != nil {
		return nil, 0, err
	}
	return &bytes, consumed, nil
}

// WriteCompactNullableBytes writes a nullable compact byte slice to an io.Writer.
func WriteCompactNullableBytes(w io.Writer, value *[]byte) error {
	_, err := w.Write(EncodeCompactNullableBytes(value))
	return err
}

// ReadCompactNullableBytes reads a nullable compact byte slice from an io.Reader.
func ReadCompactNullableBytes(r io.Reader) (*[]byte, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	bytes, err := ReadCompactBytes(r)
	if err != nil {
		return nil, err
	}
	return &bytes, nil
}

// ============================================================================
// UUID - 16-byte UUID (128-bit)
// ============================================================================

// EncodeUUID encodes a UUID to a byte slice.
func EncodeUUID(value uuid.UUID) []byte {
	return value[:]
}

// DecodeUUID decodes a UUID from a byte slice.
func DecodeUUID(data []byte) (uuid.UUID, error) {
	if len(data) < 16 {
		return uuid.Nil, ErrInsufficientData
	}
	var id uuid.UUID
	copy(id[:], data[:16])
	return id, nil
}

// WriteUUID writes a UUID to an io.Writer.
func WriteUUID(w io.Writer, value uuid.UUID) error {
	_, err := w.Write(EncodeUUID(value))
	return err
}

// ReadUUID reads a UUID from an io.Reader.
func ReadUUID(r io.Reader) (uuid.UUID, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return uuid.Nil, err
	}
	return DecodeUUID(buf)
}

// ============================================================================
// ARRAY - Array of type T, prefixed with a 32-bit length (-1 for null)
// ============================================================================

// ArrayEncoder is a function type that encodes a single array element to bytes.
type ArrayEncoder func(interface{}) ([]byte, error)

// ArrayDecoder is a function type that decodes a single array element from bytes.
// It returns the decoded element and the number of bytes consumed.
type ArrayDecoder func([]byte) (interface{}, int, error)

// EncodeArray encodes an array of elements using the provided encoder function.
// If items is nil, encodes -1 as the length.
func EncodeArray(items []interface{}, encoder ArrayEncoder) ([]byte, error) {
	if items == nil {
		return EncodeInt32(-1), nil
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		encoded, err := encoder(item)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}
	return buf, nil
}

// DecodeArray decodes an array of elements using the provided decoder function.
// It returns the decoded array and the number of bytes consumed.
func DecodeArray(data []byte, decoder ArrayDecoder) ([]interface{}, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]interface{}, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		item, consumed, err := decoder(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = item
		offset += consumed
	}
	return items, offset, nil
}

// WriteArray writes an array to an io.Writer using the provided encoder function.
func WriteArray(w io.Writer, items []interface{}, encoder ArrayEncoder) error {
	encoded, err := EncodeArray(items, encoder)
	if err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

// ReadArray reads an array from an io.Reader.
// Note: This function reads the length prefix only. To read the actual elements,
// you should read the remaining bytes and use DecodeArray with the full byte slice.
// This is because array elements can have variable sizes and the decoder needs
// to see the full data to properly decode each element.
func ReadArray(r io.Reader) (int32, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return 0, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return 0, err
	}
	return length, nil
}

// ============================================================================
// COMPACT_ARRAY - Array of type T, prefixed with an unsigned VARINT length (0 for null)
// ============================================================================

// EncodeCompactArray encodes a compact array of elements using the provided encoder function.
// If items is nil, encodes 0 as the length.
func EncodeCompactArray(items []interface{}, encoder ArrayEncoder) ([]byte, error) {
	if items == nil {
		return encodeVaruint32(0), nil
	}
	length := uint32(len(items) + 1) // +1 for the length field itself
	buf := encodeVaruint32(length)
	for _, item := range items {
		encoded, err := encoder(item)
		if err != nil {
			return nil, err
		}
		buf = append(buf, encoded...)
	}
	return buf, nil
}

// DecodeCompactArray decodes a compact array of elements using the provided decoder function.
// It returns the decoded array and the number of bytes consumed.
func DecodeCompactArray(data []byte, decoder ArrayDecoder) ([]interface{}, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length-- // Subtract 1 for the length field itself
	items := make([]interface{}, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		item, consumed, err := decoder(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = item
		offset += consumed
	}
	return items, offset, nil
}

// WriteCompactArray writes a compact array to an io.Writer using the provided encoder function.
func WriteCompactArray(w io.Writer, items []interface{}, encoder ArrayEncoder) error {
	encoded, err := EncodeCompactArray(items, encoder)
	if err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

// ReadCompactArray reads a compact array length from an io.Reader.
// Note: This function reads the length prefix only. To read the actual elements,
// you should read the remaining bytes and use DecodeCompactArray with the full byte slice.
// This is because array elements can have variable sizes and the decoder needs
// to see the full data to properly decode each element.
func ReadCompactArray(r io.Reader) (uint32, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return 0, err
	}
	if length == 0 {
		return 0, nil
	}
	if length < 1 {
		return 0, errors.New("invalid compact array length")
	}
	length-- // Subtract 1 for the length field itself
	return length, nil
}

// ============================================================================
// Typed Array Helpers - Convenience methods for primitive types
// ============================================================================

// EncodeInt8Array encodes an array of int8 values.
func EncodeInt8Array(items []int8) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeInt8(item)...)
	}
	return buf
}

// DecodeInt8Array decodes an array of int8 values.
func DecodeInt8Array(data []byte) ([]int8, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]int8, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset >= len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt8(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset++
	}
	return items, offset, nil
}

// WriteInt8Array writes an array of int8 values to an io.Writer.
func WriteInt8Array(w io.Writer, items []int8) error {
	_, err := w.Write(EncodeInt8Array(items))
	return err
}

// ReadInt8Array reads an array of int8 values from an io.Reader.
func ReadInt8Array(r io.Reader) ([]int8, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]int8, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadInt8(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeInt16Array encodes an array of int16 values.
func EncodeInt16Array(items []int16) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeInt16(item)...)
	}
	return buf
}

// DecodeInt16Array decodes an array of int16 values.
func DecodeInt16Array(data []byte) ([]int16, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]int16, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+2 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt16(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 2
	}
	return items, offset, nil
}

// WriteInt16Array writes an array of int16 values to an io.Writer.
func WriteInt16Array(w io.Writer, items []int16) error {
	_, err := w.Write(EncodeInt16Array(items))
	return err
}

// ReadInt16Array reads an array of int16 values from an io.Reader.
func ReadInt16Array(r io.Reader) ([]int16, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]int16, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadInt16(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeInt32Array encodes an array of int32 values.
func EncodeInt32Array(items []int32) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeInt32(item)...)
	}
	return buf
}

// DecodeInt32Array decodes an array of int32 values.
func DecodeInt32Array(data []byte) ([]int32, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]int32, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+4 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt32(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 4
	}
	return items, offset, nil
}

// WriteInt32Array writes an array of int32 values to an io.Writer.
func WriteInt32Array(w io.Writer, items []int32) error {
	_, err := w.Write(EncodeInt32Array(items))
	return err
}

// ReadInt32Array reads an array of int32 values from an io.Reader.
func ReadInt32Array(r io.Reader) ([]int32, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]int32, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadInt32(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeInt64Array encodes an array of int64 values.
func EncodeInt64Array(items []int64) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeInt64(item)...)
	}
	return buf
}

// DecodeInt64Array decodes an array of int64 values.
func DecodeInt64Array(data []byte) ([]int64, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]int64, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+8 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt64(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 8
	}
	return items, offset, nil
}

// WriteInt64Array writes an array of int64 values to an io.Writer.
func WriteInt64Array(w io.Writer, items []int64) error {
	_, err := w.Write(EncodeInt64Array(items))
	return err
}

// ReadInt64Array reads an array of int64 values from an io.Reader.
func ReadInt64Array(r io.Reader) ([]int64, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]int64, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadInt64(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeUint16Array encodes an array of uint16 values.
func EncodeUint16Array(items []uint16) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeUint16(item)...)
	}
	return buf
}

// DecodeUint16Array decodes an array of uint16 values.
func DecodeUint16Array(data []byte) ([]uint16, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]uint16, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+2 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUint16(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 2
	}
	return items, offset, nil
}

// WriteUint16Array writes an array of uint16 values to an io.Writer.
func WriteUint16Array(w io.Writer, items []uint16) error {
	_, err := w.Write(EncodeUint16Array(items))
	return err
}

// ReadUint16Array reads an array of uint16 values from an io.Reader.
func ReadUint16Array(r io.Reader) ([]uint16, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]uint16, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadUint16(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeUint32Array encodes an array of uint32 values.
func EncodeUint32Array(items []uint32) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeUint32(item)...)
	}
	return buf
}

// DecodeUint32Array decodes an array of uint32 values.
func DecodeUint32Array(data []byte) ([]uint32, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]uint32, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+4 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUint32(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 4
	}
	return items, offset, nil
}

// WriteUint32Array writes an array of uint32 values to an io.Writer.
func WriteUint32Array(w io.Writer, items []uint32) error {
	_, err := w.Write(EncodeUint32Array(items))
	return err
}

// ReadUint32Array reads an array of uint32 values from an io.Reader.
func ReadUint32Array(r io.Reader) ([]uint32, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]uint32, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadUint32(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeStringArray encodes an array of string values.
func EncodeStringArray(items []string) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeString(item)...)
	}
	return buf
}

// DecodeStringArray decodes an array of string values.
func DecodeStringArray(data []byte) ([]string, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]string, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		val, n, err := DecodeString(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += n
	}
	return items, offset, nil
}

// WriteStringArray writes an array of string values to an io.Writer.
func WriteStringArray(w io.Writer, items []string) error {
	_, err := w.Write(EncodeStringArray(items))
	return err
}

// ReadStringArray reads an array of string values from an io.Reader.
func ReadStringArray(r io.Reader) ([]string, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]string, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadString(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeFloat64Array encodes an array of float64 values.
func EncodeFloat64Array(items []float64) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeFloat64(item)...)
	}
	return buf
}

// DecodeFloat64Array decodes an array of float64 values.
func DecodeFloat64Array(data []byte) ([]float64, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]float64, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+8 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeFloat64(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 8
	}
	return items, offset, nil
}

// WriteFloat64Array writes an array of float64 values to an io.Writer.
func WriteFloat64Array(w io.Writer, items []float64) error {
	_, err := w.Write(EncodeFloat64Array(items))
	return err
}

// ReadFloat64Array reads an array of float64 values from an io.Reader.
func ReadFloat64Array(r io.Reader) ([]float64, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]float64, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadFloat64(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeUUIDArray encodes an array of UUID values.
func EncodeUUIDArray(items []uuid.UUID) []byte {
	if items == nil {
		return EncodeInt32(-1)
	}
	buf := EncodeInt32(int32(len(items)))
	for _, item := range items {
		buf = append(buf, EncodeUUID(item)...)
	}
	return buf
}

// DecodeUUIDArray decodes an array of UUID values.
func DecodeUUIDArray(data []byte) ([]uuid.UUID, int, error) {
	if len(data) < 4 {
		return nil, 0, ErrInsufficientData
	}
	length, err := DecodeInt32(data[:4])
	if err != nil {
		return nil, 0, err
	}
	if length == -1 {
		return nil, 4, nil
	}
	if length < 0 {
		return nil, 0, errors.New("invalid array length")
	}
	items := make([]uuid.UUID, length)
	offset := 4
	for i := int32(0); i < length; i++ {
		if offset+16 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUUID(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 16
	}
	return items, offset, nil
}

// WriteUUIDArray writes an array of UUID values to an io.Writer.
func WriteUUIDArray(w io.Writer, items []uuid.UUID) error {
	_, err := w.Write(EncodeUUIDArray(items))
	return err
}

// ReadUUIDArray reads an array of UUID values from an io.Reader.
func ReadUUIDArray(r io.Reader) ([]uuid.UUID, error) {
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err
	}
	length, err := DecodeInt32(lengthBuf)
	if err != nil {
		return nil, err
	}
	if length == -1 {
		return nil, nil
	}
	if length < 0 {
		return nil, errors.New("invalid array length")
	}
	items := make([]uuid.UUID, length)
	for i := int32(0); i < length; i++ {
		val, err := ReadUUID(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// ============================================================================
// Typed Compact Array Helpers - Convenience methods for primitive types
// ============================================================================

// EncodeCompactInt8Array encodes a compact array of int8 values.
func EncodeCompactInt8Array(items []int8) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeInt8(item)...)
	}
	return buf
}

// DecodeCompactInt8Array decodes a compact array of int8 values.
func DecodeCompactInt8Array(data []byte) ([]int8, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]int8, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset >= len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt8(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset++
	}
	return items, offset, nil
}

// WriteCompactInt8Array writes a compact array of int8 values to an io.Writer.
func WriteCompactInt8Array(w io.Writer, items []int8) error {
	_, err := w.Write(EncodeCompactInt8Array(items))
	return err
}

// ReadCompactInt8Array reads a compact array of int8 values from an io.Reader.
func ReadCompactInt8Array(r io.Reader) ([]int8, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]int8, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadInt8(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactInt16Array encodes a compact array of int16 values.
func EncodeCompactInt16Array(items []int16) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeInt16(item)...)
	}
	return buf
}

// DecodeCompactInt16Array decodes a compact array of int16 values.
func DecodeCompactInt16Array(data []byte) ([]int16, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]int16, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+2 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt16(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 2
	}
	return items, offset, nil
}

// WriteCompactInt16Array writes a compact array of int16 values to an io.Writer.
func WriteCompactInt16Array(w io.Writer, items []int16) error {
	_, err := w.Write(EncodeCompactInt16Array(items))
	return err
}

// ReadCompactInt16Array reads a compact array of int16 values from an io.Reader.
func ReadCompactInt16Array(r io.Reader) ([]int16, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]int16, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadInt16(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactInt32Array encodes a compact array of int32 values.
func EncodeCompactInt32Array(items []int32) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeInt32(item)...)
	}
	return buf
}

// DecodeCompactInt32Array decodes a compact array of int32 values.
func DecodeCompactInt32Array(data []byte) ([]int32, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]int32, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+4 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt32(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 4
	}
	return items, offset, nil
}

// WriteCompactInt32Array writes a compact array of int32 values to an io.Writer.
func WriteCompactInt32Array(w io.Writer, items []int32) error {
	_, err := w.Write(EncodeCompactInt32Array(items))
	return err
}

// ReadCompactInt32Array reads a compact array of int32 values from an io.Reader.
func ReadCompactInt32Array(r io.Reader) ([]int32, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]int32, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadInt32(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactInt64Array encodes a compact array of int64 values.
func EncodeCompactInt64Array(items []int64) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeInt64(item)...)
	}
	return buf
}

// DecodeCompactInt64Array decodes a compact array of int64 values.
func DecodeCompactInt64Array(data []byte) ([]int64, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]int64, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+8 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeInt64(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 8
	}
	return items, offset, nil
}

// WriteCompactInt64Array writes a compact array of int64 values to an io.Writer.
func WriteCompactInt64Array(w io.Writer, items []int64) error {
	_, err := w.Write(EncodeCompactInt64Array(items))
	return err
}

// ReadCompactInt64Array reads a compact array of int64 values from an io.Reader.
func ReadCompactInt64Array(r io.Reader) ([]int64, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]int64, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadInt64(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactUint16Array encodes a compact array of uint16 values.
func EncodeCompactUint16Array(items []uint16) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeUint16(item)...)
	}
	return buf
}

// DecodeCompactUint16Array decodes a compact array of uint16 values.
func DecodeCompactUint16Array(data []byte) ([]uint16, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]uint16, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+2 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUint16(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 2
	}
	return items, offset, nil
}

// WriteCompactUint16Array writes a compact array of uint16 values to an io.Writer.
func WriteCompactUint16Array(w io.Writer, items []uint16) error {
	_, err := w.Write(EncodeCompactUint16Array(items))
	return err
}

// ReadCompactUint16Array reads a compact array of uint16 values from an io.Reader.
func ReadCompactUint16Array(r io.Reader) ([]uint16, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]uint16, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadUint16(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactUint32Array encodes a compact array of uint32 values.
func EncodeCompactUint32Array(items []uint32) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeUint32(item)...)
	}
	return buf
}

// DecodeCompactUint32Array decodes a compact array of uint32 values.
func DecodeCompactUint32Array(data []byte) ([]uint32, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]uint32, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+4 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUint32(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 4
	}
	return items, offset, nil
}

// WriteCompactUint32Array writes a compact array of uint32 values to an io.Writer.
func WriteCompactUint32Array(w io.Writer, items []uint32) error {
	_, err := w.Write(EncodeCompactUint32Array(items))
	return err
}

// ReadCompactUint32Array reads a compact array of uint32 values from an io.Reader.
func ReadCompactUint32Array(r io.Reader) ([]uint32, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]uint32, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadUint32(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactStringArray encodes a compact array of string values.
func EncodeCompactStringArray(items []string) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeCompactString(item)...)
	}
	return buf
}

// DecodeCompactStringArray decodes a compact array of string values.
func DecodeCompactStringArray(data []byte) ([]string, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]string, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		val, consumed, err := DecodeCompactString(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += consumed
	}
	return items, offset, nil
}

// WriteCompactStringArray writes a compact array of string values to an io.Writer.
func WriteCompactStringArray(w io.Writer, items []string) error {
	_, err := w.Write(EncodeCompactStringArray(items))
	return err
}

// ReadCompactStringArray reads a compact array of string values from an io.Reader.
func ReadCompactStringArray(r io.Reader) ([]string, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]string, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadCompactString(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactFloat64Array encodes a compact array of float64 values.
func EncodeCompactFloat64Array(items []float64) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeFloat64(item)...)
	}
	return buf
}

// DecodeCompactFloat64Array decodes a compact array of float64 values.
func DecodeCompactFloat64Array(data []byte) ([]float64, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]float64, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+8 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeFloat64(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 8
	}
	return items, offset, nil
}

// WriteCompactFloat64Array writes a compact array of float64 values to an io.Writer.
func WriteCompactFloat64Array(w io.Writer, items []float64) error {
	_, err := w.Write(EncodeCompactFloat64Array(items))
	return err
}

// ReadCompactFloat64Array reads a compact array of float64 values from an io.Reader.
func ReadCompactFloat64Array(r io.Reader) ([]float64, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]float64, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadFloat64(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// EncodeCompactUUIDArray encodes a compact array of UUID values.
func EncodeCompactUUIDArray(items []uuid.UUID) []byte {
	if items == nil {
		return encodeVaruint32(0)
	}
	length := uint32(len(items) + 1)
	buf := encodeVaruint32(length)
	for _, item := range items {
		buf = append(buf, EncodeUUID(item)...)
	}
	return buf
}

// DecodeCompactUUIDArray decodes a compact array of UUID values.
func DecodeCompactUUIDArray(data []byte) ([]uuid.UUID, int, error) {
	length, n, err := decodeVaruint32(data)
	if err != nil {
		return nil, 0, err
	}
	if length == 0 {
		return nil, n, nil
	}
	if length < 1 {
		return nil, 0, errors.New("invalid compact array length")
	}
	length--
	items := make([]uuid.UUID, length)
	offset := n
	for i := uint32(0); i < length; i++ {
		if offset+16 > len(data) {
			return nil, 0, ErrInsufficientData
		}
		val, err := DecodeUUID(data[offset:])
		if err != nil {
			return nil, 0, err
		}
		items[i] = val
		offset += 16
	}
	return items, offset, nil
}

// WriteCompactUUIDArray writes a compact array of UUID values to an io.Writer.
func WriteCompactUUIDArray(w io.Writer, items []uuid.UUID) error {
	_, err := w.Write(EncodeCompactUUIDArray(items))
	return err
}

// ReadCompactUUIDArray reads a compact array of UUID values from an io.Reader.
func ReadCompactUUIDArray(r io.Reader) ([]uuid.UUID, error) {
	length, err := ReadVaruint32(r)
	if err != nil {
		return nil, err
	}
	if length == 0 {
		return nil, nil
	}
	if length < 1 {
		return nil, errors.New("invalid compact array length")
	}
	length--
	items := make([]uuid.UUID, length)
	for i := uint32(0); i < length; i++ {
		val, err := ReadUUID(r)
		if err != nil {
			return nil, err
		}
		items[i] = val
	}
	return items, nil
}

// ============================================================================
// RECORDS - Sequence of Kafka records as NULLABLE_BYTES
// ============================================================================

// EncodeRecords encodes a sequence of Kafka records (as bytes) using NULLABLE_BYTES encoding.
func EncodeRecords(records []byte) []byte {
	if records == nil {
		return EncodeNullableBytes(nil)
	}
	return EncodeNullableBytes(&records)
}

// DecodeRecords decodes a sequence of Kafka records from bytes.
// It returns the records and the number of bytes consumed.
func DecodeRecords(data []byte) ([]byte, int, error) {
	records, n, err := DecodeNullableBytes(data)
	if err != nil {
		return nil, 0, err
	}
	if records == nil {
		return nil, n, nil
	}
	return *records, n, nil
}

// WriteRecords writes a sequence of Kafka records to an io.Writer.
func WriteRecords(w io.Writer, records []byte) error {
	return WriteNullableBytes(w, &records)
}

// ReadRecords reads a sequence of Kafka records from an io.Reader.
func ReadRecords(r io.Reader) ([]byte, error) {
	records, err := ReadNullableBytes(r)
	if err != nil {
		return nil, err
	}
	if records == nil {
		return nil, nil
	}
	return *records, nil
}

// ============================================================================
// COMPACT_RECORDS - Sequence of Kafka records as COMPACT_NULLABLE_BYTES
// ============================================================================

// EncodeCompactRecords encodes a sequence of Kafka records (as bytes) using COMPACT_NULLABLE_BYTES encoding.
func EncodeCompactRecords(records []byte) []byte {
	if records == nil {
		return EncodeCompactNullableBytes(nil)
	}
	return EncodeCompactNullableBytes(&records)
}

// DecodeCompactRecords decodes a sequence of Kafka records from bytes.
// It returns the records and the number of bytes consumed.
func DecodeCompactRecords(data []byte) ([]byte, int, error) {
	records, n, err := DecodeCompactNullableBytes(data)
	if err != nil {
		return nil, 0, err
	}
	if records == nil {
		return nil, n, nil
	}
	return *records, n, nil
}

// WriteCompactRecords writes a sequence of Kafka records to an io.Writer.
func WriteCompactRecords(w io.Writer, records []byte) error {
	return WriteCompactNullableBytes(w, &records)
}

// ReadCompactRecords reads a sequence of Kafka records from an io.Reader.
func ReadCompactRecords(r io.Reader) ([]byte, error) {
	records, err := ReadCompactNullableBytes(r)
	if err != nil {
		return nil, err
	}
	if records == nil {
		return nil, nil
	}
	return *records, nil
}
