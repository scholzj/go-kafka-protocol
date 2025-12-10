package protocol

import (
	"encoding/binary"
	"fmt"
)

////////////////////
// Structs to decode the headers
////////////////////

type RequestHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
}

type Request struct {
	RequestHeader
	Size int32
	Body []byte
}

type ResponseHeader struct {
	ApiKey        int16
	ApiVersion    int16
	CorrelationId int32
	ClientId      *string
}

type Response struct {
	ResponseHeader
	Size int32
	Body []byte
}

////////////////////
// Decoding anf encoding methods for primitive types
////////////////////

func DecodeInt8(bytes []byte) (int8, int, error) {
	var v int8
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeBool(bytes []byte) (bool, int, error) {
	v, n, err := DecodeInt8(bytes)
	return v != 0, n, err
}

func DecodeInt16(bytes []byte) (int16, int, error) {
	var v int16
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeInt32(bytes []byte) (int32, int, error) {
	var v int32
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeInt64(bytes []byte) (int64, int, error) {
	var v int64
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeUInt16(bytes []byte) (uint16, int, error) {
	var v uint16
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeUInt32(bytes []byte) (uint32, int, error) {
	var v uint32
	n, err := binary.Decode(bytes, binary.BigEndian, &v)
	return v, n, err
}

func DecodeVarint(bytes []byte) (int64, int, error) {
	v, n := binary.Varint(bytes)
	return v, n, nil
}

func DecodeUvarint(bytes []byte) (uint64, int, error) {
	v, n := binary.Uvarint(bytes)
	return v, n, nil
}

func DecodeNullableString(bytes []byte) (*string, int, error) {
	offset := 0
	length, c, err := DecodeInt16(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode string length", err)
		return nil, 0, err
	}
	offset += c

	if length < 0 {
		return nil, offset, nil
	} else if length == 0 {
		emptyString := ""
		return &emptyString, offset, nil
	} else {
		str := string(bytes[offset : offset+int(length)])
		offset += int(length)
		return &str, offset, nil
	}
}

func DecodeCompactString(bytes []byte) (string, int, error) {
	offset := 0

	length, c, err := DecodeUvarint(bytes[offset:])
	if err != nil {
		return "", 0, err
	}
	offset += c

	length-- // subtract 1 for the length byte based on the Kafka protocol spec
	fmt.Printf("Decoded length: %d; bytes read: %d; overall bytes length: %d\n", length, c, len(bytes[offset:]))

	if length < 0 {
		return "", 0, fmt.Errorf("invalid compact string length %d", length)
	} else if length == 0 {
		return "", 0, nil
	} else {
		return string(bytes[offset : offset+int(length)]), offset + int(length), nil
	}
}

func DecodeCompactNullableString(bytes []byte) (*string, int, error) {
	offset := 0

	length, c, err := DecodeUvarint(bytes[offset:])
	if err != nil {
		return nil, 0, err
	}
	offset += c

	length-- // subtract 1 for the length byte based on the Kafka protocol spec
	fmt.Printf("Decoded length: %d; bytes read: %d; overall bytes length: %d\n", length, c, len(bytes[offset:]))

	if length < 0 {
		return nil, 0, fmt.Errorf("invalid compact string length %d", length)
	} else if length == 0 {
		return nil, 0, nil
	} else {
		str := string(bytes[offset : offset+int(length)])
		return &str, offset + int(length), nil
	}
}

// Function used to decode structured arrays
type ArrayDecoder[T interface{}] func([]byte) (T, int, error)

func DecodeCompactArray[T interface{}](bytes []byte, decoder ArrayDecoder[T]) ([]T, int, error) {
	offset := 0

	length, c, err := DecodeUvarint(bytes[offset:])
	if err != nil {
		return nil, offset, err
	}
	offset += c

	length-- // We remove one according to the Kafka spec

	array := make([]T, length)
	for i := 0; i < int(length); i++ {
		array[i], c, err = decoder(bytes[offset:])
		if err != nil {
			return nil, offset, err
		}

		offset += c
	}

	return array, offset, nil
}

////////////////////
// Tagged fields methods
////////////////////

func DecodeRawTaggedFields(bytes []byte) ([][]byte, int, error) {
	offset := 0

	// Find the number of tags
	l, c, err := DecodeUvarint(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return nil, offset, err
	}
	offset += c
	fmt.Printf("Raw tagged fields length: %d bytes, %d records\n", c, l)

	// Read each tag and store it in the slice of slices
	//(we need to partially decode them to know how many bytes are the raw tags)
	rawTaggedFields := make([][]byte, l)
	for i := 0; i < int(l); i++ {
		// Read the tag number first
		tag, c, err := DecodeUvarint(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag", err)
			return nil, offset, err
		}

		fmt.Printf("Found tag %d\n", tag)
		rawTaggedFields[i] = bytes[offset : offset+c]
		offset += c

		// Read the tag length
		tagLength, c, err := DecodeUvarint(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag length", err)
			return nil, offset, err
		}
		offset += c

		fmt.Printf("Found tag length %d bytes\n", tagLength)
		rawTaggedFields[i] = append(rawTaggedFields[i], bytes[offset:offset+c]...)
		offset += c

		// Copy the raw tag value bytes without decoding them
		rawTaggedFields[i] = append(rawTaggedFields[i], bytes[offset:offset+int(tagLength)]...)
		offset += int(tagLength)
	}

	return rawTaggedFields, offset, nil
}

// Uses the request/response message as parameter to set the tagged fields
type TaggedFieldsDecoder[T interface{}] func([]byte, *T, uint64, uint64) (int, error)

func DecodeTaggedFields[T interface{}](bytes []byte, decoder TaggedFieldsDecoder[T], msg *T) (int, error) {
	offset := 0

	// Find the number of tags
	l, c, err := DecodeUvarint(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return offset, err
	}
	offset += c
	fmt.Printf("Tagged fields length: %d bytes, %d records\n", c, l)

	for i := 0; i < int(l); i++ {
		// Read the tag number first
		tag, c, err := DecodeUvarint(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag", err)
			return offset, err
		}
		offset += c

		fmt.Printf("Found tag %d\n", tag)

		// Read the tag length
		tagLength, c, err := DecodeUvarint(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tag length", err)
			return offset, err
		}
		offset += c

		fmt.Printf("Found tag length %d bytes\n", tagLength)

		// Use the decoded to decode the fields
		c, err = decoder(bytes[offset:], msg, tag, tagLength)
		if err != nil {
			fmt.Println("Failed to decode tag in decoder", err)
			return offset, err
		}
		offset += c
	}

	return offset, nil
}
