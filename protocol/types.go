package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

////////////////////
// Decoding and encoding methods for primitive types
////////////////////

func WriteInt8(w io.Writer, value int8) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt8(r io.Reader) (int8, error) {
	var v int8
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeInt8(bytes []byte) (int8, int, error) {
//	var v int8
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

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

//func DecodeBool(bytes []byte) (bool, int, error) {
//	v, n, err := DecodeInt8(bytes)
//	return v != 0, n, err
//}

func WriteInt16(w io.Writer, value int16) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt16(r io.Reader) (int16, error) {
	var v int16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeInt16(bytes []byte) (int16, int, error) {
//	var v int16
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

func WriteInt32(w io.Writer, value int32) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeInt32(bytes []byte) (int32, int, error) {
//	var v int32
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

func WriteInt64(w io.Writer, value int64) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadInt64(r io.Reader) (int64, error) {
	var v int64
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeInt64(bytes []byte) (int64, int, error) {
//	var v int64
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

func WriteUint16(w io.Writer, value uint16) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadUInt16(r io.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeUInt16(bytes []byte) (uint16, int, error) {
//	var v uint16
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

func WriteUint32(w io.Writer, value uint32) error {
	return binary.Write(w, binary.BigEndian, value)
}

func ReadUInt32(r io.Reader) (uint32, error) {
	var v uint32
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}

//func DecodeUInt32(bytes []byte) (uint32, int, error) {
//	var v uint32
//	n, err := binary.Decode(bytes, binary.BigEndian, &v)
//	return v, n, err
//}

func WriteVarint(w io.Writer, value int64) error {
	buf := make([]byte, binary.MaxVarintLen64)
	n := binary.PutVarint(buf, value)
	_, err := w.Write(buf[:n])
	return err
}

func ReadVarint(r io.Reader) (int64, error) {
	var result int64
	var shift uint

	for {
		buf := make([]byte, 1)
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}
		b := buf[0]
		result |= int64(b&0x7F) << shift
		if (b & 0x80) == 0 {
			return result, nil
		}
		shift += 7
		if shift >= 32 {
			return 0, errors.New("invalid Varint")
		}
	}
}

//func DecodeVarint(bytes []byte) (int64, int, error) {
//	v, n := binary.Varint(bytes)
//	return v, n, nil
//}

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

//func DecodeUvarint(bytes []byte) (uint64, int, error) {
//	v, n := binary.Uvarint(bytes)
//	return v, n, nil
//}

func WriteUUID(w io.Writer, value uuid.UUID) error {
	//b, err := value.MarshalBinary()
	//if err != nil {
	//	return err
	//}

	_, err := w.Write(value[:])
	if err != nil {
		return err
	}

	return nil
}

func ReadUUID(r io.Reader) (uuid.UUID, error) {
	buf := make([]byte, 16)
	if _, err := io.ReadFull(r, buf); err != nil {
		fmt.Println("Failed to read UUID", err)
		return uuid.Nil, err
	}

	//var id uuid.UUID
	//err := id.UnmarshalBinary(buf)
	//if err != nil {
	//	return id, err
	//}

	return uuid.FromBytes(buf)
}

func WriteString(w io.Writer, value string) error {
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
		fmt.Println("Failed to read string length", err)
		return "", err
	}

	if length < 0 {
		return "", fmt.Errorf("invalid string length %d", length)
	} else if length == 0 {
		return "", nil
	} else {
		var bytes = make([]byte, length)
		_, err = r.Read(bytes)
		if err != nil {
			fmt.Println("Failed to read string length", err)
			return "", err
		}

		return string(bytes), nil
	}
}

func WriteNullableString(w io.Writer, value *string) error {
	if value == nil {
		return WriteInt16(w, -1)
	} else {
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
		fmt.Println("Failed to read string length", err)
		return nil, err
	}

	if length < 0 {
		return nil, nil
	} else if length == 0 {
		emptyString := ""
		return &emptyString, nil
	} else {
		var bytes = make([]byte, length)
		_, err = r.Read(bytes)
		if err != nil {
			fmt.Println("Failed to read string length", err)
			return nil, err
		}

		str := string(bytes)
		return &str, nil
	}
}

//func DecodeNullableString(bytes []byte) (*string, int, error) {
//	offset := 0
//	length, c, err := DecodeInt16(bytes[offset:])
//	if err != nil {
//		fmt.Println("Failed to decode string length", err)
//		return nil, 0, err
//	}
//	offset += c
//
//	if length < 0 {
//		return nil, offset, nil
//	} else if length == 0 {
//		emptyString := ""
//		return &emptyString, offset, nil
//	} else {
//		str := string(bytes[offset : offset+int(length)])
//		offset += int(length)
//		return &str, offset, nil
//	}
//}

func WriteCompactString(w io.Writer, value string) error {
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
		var bytes = make([]byte, length)
		_, err = r.Read(bytes)
		if err != nil {
			fmt.Println("Failed to read string length", err)
			return "", err
		}

		return string(bytes), nil
	}
}

//func DecodeCompactString(bytes []byte) (string, int, error) {
//	offset := 0
//
//	length, c, err := DecodeUvarint(bytes[offset:])
//	if err != nil {
//		return "", 0, err
//	}
//	offset += c
//
//	length-- // subtract 1 for the length byte based on the Kafka protocol spec
//
//	if length < 0 {
//		return "", 0, fmt.Errorf("invalid compact string length %d", length)
//	} else if length == 0 {
//		return "", 0, nil
//	} else {
//		return string(bytes[offset : offset+int(length)]), offset + int(length), nil
//	}
//}

func WriteNullableCompactString(w io.Writer, value *string) error {
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

	if length < 0 {
		return nil, fmt.Errorf("invalid compact string length %d", length)
	} else if length == 0 {
		return nil, nil
	} else if length == 1 {
		var str = ""
		return &str, nil
	} else {
		length-- // subtract 1 for the length byte based on the Kafka protocol spec
		var bytes = make([]byte, length)
		_, err = r.Read(bytes)
		if err != nil {
			fmt.Println("Failed to read string length", err)
			return nil, err
		}

		var str = string(bytes)
		return &str, nil
	}
}

//func DecodeCompactNullableString(bytes []byte) (*string, int, error) {
//	offset := 0
//
//	length, c, err := DecodeUvarint(bytes[offset:])
//	if err != nil {
//		return nil, 0, err
//	}
//	offset += c
//
//	length-- // subtract 1 for the length byte based on the Kafka protocol spec
//
//	if length < 0 {
//		return nil, 0, fmt.Errorf("invalid compact string length %d", length)
//	} else if length == 0 {
//		return nil, 0, nil
//	} else {
//		str := string(bytes[offset : offset+int(length)])
//		return &str, offset + int(length), nil
//	}
//}

// Function used to decode structured arrays
// type ArrayDecoder[T interface{}] func([]byte) (T, int, error)
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

func ReadArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) ([]T, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, err
	}

	if length <= 0 {
		return make([]T, 0), nil
	} else {
		array := make([]T, length)
		for i := 0; i < int(length); i++ {
			array[i], err = decoder(r)
			if err != nil {
				return nil, err
			}
		}

		return array, nil
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
		array := make([]T, length)
		for i := 0; i < int(length); i++ {
			array[i], err = decoder(r)
			if err != nil {
				return nil, err
			}
		}

		return &array, nil
	}
}

//func WriteCompactArray[T interface{}](w io.Writer, encoder ArrayEncoder[T], values []T) error {
//	err := WriteUvarint(w, uint64(len(values))+1)
//	if err != nil {
//		return err
//	}
//
//	for _, v := range values {
//		err := encoder(w, v)
//		if err != nil {
//			return err
//		}
//	}
//
//	return nil
//}

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

//
//func ReadCompactArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) ([]T, error) {
//	length, err := ReadUvarint(r)
//	if err != nil {
//		return nil, err
//	}
//
//	if length <= 0 {
//		return make([]T, 0), nil
//	} else {
//		length-- // We remove one according to the Kafka spec
//
//		array := make([]T, length)
//		for i := 0; i < int(length); i++ {
//			array[i], err = decoder(r)
//			if err != nil {
//				return nil, err
//			}
//		}
//
//		return array, nil
//	}
//}

func ReadNullableCompactArray[T interface{}](r io.Reader, decoder ArrayReaderDecoder[T]) (*[]T, error) {
	length, err := ReadUvarint(r)
	if err != nil {
		return nil, err
	}

	if length == 0 {
		return nil, nil
		//} else if length == 0 {
		//	array := make([]T, 0)
		//	return &array, nil
	} else {
		length-- // We remove one according to the Kafka spec

		array := make([]T, length)
		for i := 0; i < int(length); i++ {
			array[i], err = decoder(r)
			if err != nil {
				return nil, err
			}
		}

		return &array, nil
	}
}

//func DecodeCompactArray[T interface{}](bytes []byte, decoder ArrayDecoder[T]) ([]T, int, error) {
//	offset := 0
//
//	length, c, err := DecodeUvarint(bytes[offset:])
//	if err != nil {
//		return nil, offset, err
//	}
//	offset += c
//
//	length-- // We remove one according to the Kafka spec
//
//	array := make([]T, length)
//	for i := 0; i < int(length); i++ {
//		array[i], c, err = decoder(bytes[offset:])
//		if err != nil {
//			return nil, offset, err
//		}
//
//		offset += c
//	}
//
//	return array, offset, nil
//}
