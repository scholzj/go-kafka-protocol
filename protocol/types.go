package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

// maxPrealloc caps how many bytes or array elements are pre-allocated for a length-prefixed field
// before any of its data has been read. Wire lengths are attacker-controlled, so allocating
// make([]T, length) up front lets a single corrupt or malicious length field trigger an enormous
// allocation (a denial of service) - especially for arrays of structs. With this cap the field is
// still decoded to its true length, but the storage grows as bytes actually arrive; a length larger
// than the data available simply makes the underlying read fail with an unexpected-EOF error.
const maxPrealloc = 4096

// maxStringLen is the largest length Kafka permits for a STRING / COMPACT_STRING field. It matches
// Short.MAX_VALUE: a non-compact string is length-prefixed with an int16, so anything longer would
// overflow the prefix (the int16 cast wraps to a negative length) and produce invalid wire data.
const maxStringLen = 32767

// maxBytesLen is the largest length Kafka permits for a BYTES / COMPACT_BYTES field, whose
// non-compact form is length-prefixed with an int32.
const maxBytesLen = 2147483647

// preallocLen returns a safe initial capacity for a field whose declared length is length.
func preallocLen(length int) int {
	if length < 0 {
		return 0 // a wire length that overflowed int is never a safe capacity
	}
	if length < maxPrealloc {
		return length
	}
	return maxPrealloc
}

// readBytesLimited reads exactly length bytes from r without pre-allocating the full length up
// front, so a corrupt or huge length cannot cause a giant allocation before any data is read.
func readBytesLimited(r io.Reader, length int) ([]byte, error) {
	if length < 0 {
		return nil, fmt.Errorf("invalid length %d", length)
	}
	if length == 0 {
		return []byte{}, nil
	}
	buf := bytes.NewBuffer(make([]byte, 0, preallocLen(length)))
	if _, err := io.CopyN(buf, r, int64(length)); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

////////////////////
// Decoding and encoding methods for primitive types
////////////////////

func WriteBool(w io.Writer, value bool) error {
	var intValue int8

	if value {
		intValue = 1
	} else {
		intValue = 0
	}

	return binary.Write(w, binary.BigEndian, intValue)
}

func ReadBool(r io.Reader) (bool, error) {
	var v int8
	err := binary.Read(r, binary.BigEndian, &v)
	return v != 0, err
}

func WriteInt8(w io.Writer, value int8) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt8(r io.Reader) (int8, error) {
	var v int8
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteInt16(w io.Writer, value int16) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt16(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteInt32(w io.Writer, value int32) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteInt64(w io.Writer, value int64) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt64(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteUint16(w io.Writer, value uint16) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadUInt16(r io.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteUint32(w io.Writer, value uint32) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadUInt32(r io.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteVarint(w io.Writer, value int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, value)
	_, err := w.Write(buf[:n])
	return err
}

func ReadVarint(r io.Reader) (int64, error) {
	// Use binary.ReadVarint to match binary.PutVarint used in WriteVarint
	// This handles the zigzag encoding properly
	var buf [binary.MaxVarintLen64]byte
	var n int
	var err error

	// Read one byte at a time until we have a complete varint
	for n = 0; n < len(buf); n++ {
		_, err = io.ReadFull(r, buf[n:n+1])
		if err != nil {
			return 0, err
		}
		if buf[n]&0x80 == 0 {
			n++
			break
		}
	}

	if n == 0 {
		return 0, errors.New("invalid Varint: no data")
	}

	value, _ := binary.Varint(buf[:n])
	return value, nil
}

func WriteUvarint(w io.Writer, value uint64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutUvarint(buf, value)
	_, err := w.Write(buf[:n])
	return err
}

func ReadUvarint(r io.Reader) (uint64, error) {
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
			return result, nil
		}
		shift += 7
		if shift >= 32 {
			return 0, errors.New("invalid Uvarint")
		}
	}
}

//func WriteVarlong(w io.Writer, value int64) error {
//	// Zig-zag encoding: (n << 1) ^ (n >> 63)
//	encoded := uint64((value << 1) ^ (value >> 63))
//	return WriteUvarint(w, encoded)
//}
//
//func ReadVarlong(r io.Reader) (int64, error) {
//	encoded, err := ReadUvarint(r)
//	if err != nil {
//		return 0, err
//	}
//	// Zig-zag decoding: (n >> 1) ^ -(n & 1)
//	value := int64((encoded >> 1) ^ uint64((int64(encoded&1)<<63)>>63))
//	return value, nil
//}

func WriteVarlong(w io.Writer, value int64) error {
	// Zigzag encoding: (value << 1) ^ (value >> 63)
	encoded := uint64((value << 1) ^ (value >> 63))
	var buf []byte

	for encoded >= 0x80 {
		buf = append(buf, byte(encoded)|0x80)
		encoded >>= 7
	}
	buf = append(buf, byte(encoded))

	_, err := w.Write(buf)
	return err
}

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
			// Zigzag decoding: (value >> 1) ^ -(value & 1)
			decoded := int64(result>>1) ^ -(int64(result) & 1)
			return decoded, nil
		}
		shift += 7
		if shift >= 64 {
			return 0, errors.New("invalid Varlong")
		}
	}
}

func WriteUUID(w io.Writer, value uuid.UUID) error {
	_, err := w.Write(value[:])
	if err != nil {
		return err
	}

	return nil
}

func ReadUUID(r io.Reader) (uuid.UUID, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r, buf); err != nil {
		return uuid.Nil, err
	}

	return uuid.FromBytes(buf)
}

//func WriteFloat64(w io.Writer, value float64) error {
//	bits := math.Float64bits(value)
//	return binary.Write(w, binary.BigEndian, bits)
//}
//
//func ReadFloat64(r io.Reader) (float64, error) {
//	var bits uint64
//	err := binary.Read(r, binary.BigEndian, &bits)
//	return math.Float64frombits(bits), err
//}

func WriteFloat64(w io.Writer, value float64) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadFloat64(r io.Reader) (float64, error) {
	var v float64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

func WriteString(w io.Writer, value string) error {
	if len(value) > maxStringLen {
		return fmt.Errorf("string length %d exceeds maximum %d", len(value), maxStringLen)
	}

	err := WriteInt16(w, int16(len(value)))
	if err != nil {
		return err
	}

	_, err = w.Write([]byte(value))
	if err != nil {
		return err
	}

	return nil
}

func ReadString(r io.Reader) (string, error) {
	length, err := ReadInt16(r)
	if err != nil {
		return "", err
	}

	if length < 0 {
		return "", fmt.Errorf("invalid string length %d", length)
	} else if length == 0 {
		return "", nil
	} else {
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return "", err
		}

		return string(b), nil
	}
}

func WriteNullableString(w io.Writer, value *string) error {
	if value == nil {
		return WriteInt16(w, -1)
	} else {
		if len(*value) > maxStringLen {
			return fmt.Errorf("string length %d exceeds maximum %d", len(*value), maxStringLen)
		}

		err := WriteInt16(w, int16(len(*value)))
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(*value))
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadNullableString(r io.Reader) (*string, error) {
	length, err := ReadInt16(r)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, nil
	} else if length == 0 {
		emptyString := ""
		return &emptyString, nil
	} else {
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return nil, err
		}

		str := string(b)
		return &str, nil
	}
}

func WriteCompactString(w io.Writer, value string) error {
	if len(value) > maxStringLen {
		return fmt.Errorf("string length %d exceeds maximum %d", len(value), maxStringLen)
	}

	if value == "" {
		return WriteUvarint(w, 1)
	} else {
		err := WriteUvarint(w, uint64(len(value))+1)
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(value))
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadCompactString(r io.Reader) (string, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return "", err
	}

	if length <= 0 {
		return "", fmt.Errorf("invalid compact string length %d", length)
	} else if length == 1 {
		return "", nil
	} else {
		length-- // subtract 1 for the length byte based on the Kafka protocol spec
		if length > maxStringLen {
			return "", fmt.Errorf("compact string length %d exceeds maximum %d", length, maxStringLen)
		}
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return "", err
		}

		return string(b), nil
	}
}

func WriteNullableCompactString(w io.Writer, value *string) error {
	if value != nil && len(*value) > maxStringLen {
		return fmt.Errorf("string length %d exceeds maximum %d", len(*value), maxStringLen)
	}

	if value == nil {
		return WriteUvarint(w, 0)
	} else if *value == "" {
		return WriteUvarint(w, 1)
	} else {
		err := WriteUvarint(w, uint64(len(*value))+1)
		if err != nil {
			return err
		}

		_, err = w.Write([]byte(*value))
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadNullableCompactString(r io.Reader) (*string, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	} else if length == 1 {
		var str = ""
		return &str, nil
	} else {
		length-- // subtract 1 for the length byte based on the Kafka protocol spec
		if length > maxStringLen {
			return nil, fmt.Errorf("compact string length %d exceeds maximum %d", length, maxStringLen)
		}
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return nil, err
		}

		var str = string(b)
		return &str, nil
	}
}

func WriteBytes(w io.Writer, value []byte) error {
	if len(value) > maxBytesLen {
		return fmt.Errorf("bytes length %d exceeds maximum %d", len(value), maxBytesLen)
	}

	err := WriteInt32(w, int32(len(value)))
	if err != nil {
		return err
	}

	if len(value) > 0 {
		_, err = w.Write(value)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadBytes(r io.Reader) ([]byte, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, fmt.Errorf("invalid bytes length %d", length)
	} else if length == 0 {
		return make([]byte, 0), nil
	} else {
		return readBytesLimited(r, int(length))
	}
}

func WriteCompactBytes(w io.Writer, value []byte) error {
	if len(value) > maxBytesLen {
		return fmt.Errorf("bytes length %d exceeds maximum %d", len(value), maxBytesLen)
	}

	if len(value) == 0 {
		return WriteUvarint(w, 1)
	} else {
		err := WriteUvarint(w, uint64(len(value))+1)
		if err != nil {
			return err
		}

		_, err = w.Write(value)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadCompactBytes(r io.Reader) ([]byte, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, fmt.Errorf("invalid compact bytes length %d", length)
	} else if length == 1 {
		return make([]byte, 0), nil
	} else {
		length-- // subtract 1 for the length byte based on the Kafka protocol spec
		if length > maxBytesLen {
			return nil, fmt.Errorf("compact bytes length %d exceeds maximum %d", length, maxBytesLen)
		}
		return readBytesLimited(r, int(length))
	}
}

func WriteNullableBytes(w io.Writer, value *[]byte) error {
	if value == nil {
		return WriteInt32(w, -1)
	} else {
		if len(*value) > maxBytesLen {
			return fmt.Errorf("bytes length %d exceeds maximum %d", len(*value), maxBytesLen)
		}

		err := WriteInt32(w, int32(len(*value)))
		if err != nil {
			return err
		}

		if len(*value) > 0 {
			_, err = w.Write(*value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ReadNullableBytes(r io.Reader) (*[]byte, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, nil
	} else if length == 0 {
		emptyBytes := make([]byte, 0)
		return &emptyBytes, nil
	} else {
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return nil, err
		}

		return &b, nil
	}
}

func WriteNullableCompactBytes(w io.Writer, value *[]byte) error {
	if value != nil && len(*value) > maxBytesLen {
		return fmt.Errorf("bytes length %d exceeds maximum %d", len(*value), maxBytesLen)
	}

	if value == nil {
		return WriteUvarint(w, 0)
	} else if len(*value) == 0 {
		return WriteUvarint(w, 1)
	} else {
		err := WriteUvarint(w, uint64(len(*value))+1)
		if err != nil {
			return err
		}

		_, err = w.Write(*value)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadNullableCompactBytes(r io.Reader) (*[]byte, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	} else if length == 1 {
		emptyBytes := make([]byte, 0)
		return &emptyBytes, nil
	} else {
		length-- // subtract 1 for the length byte based on the Kafka protocol spec
		if length > maxBytesLen {
			return nil, fmt.Errorf("compact bytes length %d exceeds maximum %d", length, maxBytesLen)
		}
		b, err := readBytesLimited(r, int(length))
		if err != nil {
			return nil, err
		}

		return &b, nil
	}
}

func WriteRecords(w io.Writer, value *[]byte) error {
	return WriteNullableBytes(w, value)
}

func ReadRecords(r io.Reader) (*[]byte, error) {
	return ReadNullableBytes(r)
}

func WriteCompactRecords(w io.Writer, value *[]byte) error {
	return WriteNullableCompactBytes(w, value)
}

func ReadCompactRecords(r io.Reader) (*[]byte, error) {
	return ReadNullableCompactBytes(r)
}

// ReadRecordsStrict / ReadCompactRecordsStrict read a non-nullable records field, rejecting a wire
// null (a negative or zero length) instead of decoding it to nil. ReadRecords / ReadCompactRecords
// are the nullable variants.
func ReadRecordsStrict(r io.Reader) ([]byte, error) {
	return ReadBytes(r)
}

func ReadCompactRecordsStrict(r io.Reader) ([]byte, error) {
	return ReadCompactBytes(r)
}

type ArrayReaderDecoder[T interface{}] func(io.Reader) (T, error)
type ArrayEncoder[T interface{}] func(io.Writer, T) error

func WriteArray[T interface{}](w io.Writer, encoder ArrayEncoder[T], values []T) error {
	err := WriteInt32(w, int32(len(values)))
	if err != nil {
		return err
	}

	for _, v := range values {
		err := encoder(w, v)
		if err != nil {
			return err
		}
	}

	return nil
}

func ReadArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) ([]T, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		// A negative (null) length is only valid for a nullable array; this is the non-nullable
		// reader, so reject it rather than silently treating it as empty.
		return nil, fmt.Errorf("invalid array length %d", length)
	} else if length == 0 {
		return make([]T, 0), nil
	} else {
		array := make([]T, 0, preallocLen(int(length)))
		for i := 0; i < int(length); i++ {
			v, err := decoder(r)
			if err != nil {
				return nil, err
			}
			array = append(array, v)
		}

		return array, nil
	}
}

func WriteNullableArray[T interface{}](w io.Writer, encoder ArrayEncoder[T], values *[]T) error {
	if values == nil {
		return WriteInt32(w, int32(-1))
	} else {
		err := WriteInt32(w, int32(len(*values)))
		if err != nil {
			return err
		}

		for _, v := range *values {
			err := encoder(w, v)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func ReadNullableArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) (*[]T, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if length < 0 {
		return nil, nil
	} else if length == 0 {
		array := make([]T, 0)
		return &array, nil
	} else {
		array := make([]T, 0, preallocLen(int(length)))
		for i := 0; i < int(length); i++ {
			v, err := decoder(r)
			if err != nil {
				return nil, err
			}
			array = append(array, v)
		}

		return &array, nil
	}
}

func WriteNullableCompactArray[T interface{}](w io.Writer, encoder ArrayEncoder[T], values *[]T) error {
	if values == nil {
		return WriteUvarint(w, uint64(0))
	} else {
		err := WriteUvarint(w, uint64(len(*values))+1)
		if err != nil {
			return err
		}

		for _, v := range *values {
			err := encoder(w, v)
			if err != nil {
				return err
			}
		}

		return nil
	}
}

func ReadNullableCompactArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) (*[]T, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
	} else {
		length-- // We remove one according to the Kafka spec

		n := int(length)
		if n < 0 {
			return nil, fmt.Errorf("invalid compact array length %d", length)
		}
		array := make([]T, 0, preallocLen(n))
		for i := 0; i < n; i++ {
			v, err := decoder(r)
			if err != nil {
				return nil, err
			}
			array = append(array, v)
		}

		return &array, nil
	}
}

// ReadCompactArray reads a non-nullable compact (flexible) array. A wire null (length 0) is invalid
// for a non-nullable field and is rejected rather than silently decoded as an empty array.
func ReadCompactArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) ([]T, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, fmt.Errorf("invalid compact array length 0 for non-nullable array")
	}

	length-- // the wire length is the element count plus one (0 is reserved for null)

	n := int(length)
	if n < 0 {
		return nil, fmt.Errorf("invalid compact array length %d", length)
	}
	array := make([]T, 0, preallocLen(n))
	for i := 0; i < n; i++ {
		v, err := decoder(r)
		if err != nil {
			return nil, err
		}
		array = append(array, v)
	}

	return array, nil
}
