package protocol

import (
	"bytes"
	"math"
	"testing"

	"github.com/google/uuid"
)

// ============================================================================
// BOOLEAN Tests
// ============================================================================

func TestBool_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value bool
	}{
		{"false", false},
		{"true", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeBool(tt.value)
			decoded, err := DecodeBool(encoded)
			if err != nil {
				t.Fatalf("DecodeBool failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeBool() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestBool_WriteRead(t *testing.T) {
	value := true
	var buf bytes.Buffer

	if err := WriteBool(&buf, value); err != nil {
		t.Fatalf("WriteBool failed: %v", err)
	}

	decoded, err := ReadBool(&buf)
	if err != nil {
		t.Fatalf("ReadBool failed: %v", err)
	}

	if decoded != value {
		t.Errorf("ReadBool() = %v, want %v", decoded, value)
	}

	// Test false
	buf.Reset()
	value = false
	if err := WriteBool(&buf, value); err != nil {
		t.Fatalf("WriteBool failed: %v", err)
	}

	decoded, err = ReadBool(&buf)
	if err != nil {
		t.Fatalf("ReadBool failed: %v", err)
	}

	if decoded != value {
		t.Errorf("ReadBool() = %v, want %v", decoded, value)
	}
}

func TestBool_DecodeInsufficientData(t *testing.T) {
	_, err := DecodeBool([]byte{})
	if err != ErrInsufficientData {
		t.Errorf("DecodeBool() error = %v, want %v", err, ErrInsufficientData)
	}
}

// ============================================================================
// INT8 Tests
// ============================================================================

func TestInt8_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int8
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"max", 127},
		{"min", -128},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt8(tt.value)
			decoded, err := DecodeInt8(encoded)
			if err != nil {
				t.Fatalf("DecodeInt8 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeInt8() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestInt8_WriteRead(t *testing.T) {
	value := int8(42)
	var buf bytes.Buffer

	if err := WriteInt8(&buf, value); err != nil {
		t.Fatalf("WriteInt8 failed: %v", err)
	}

	decoded, err := ReadInt8(&buf)
	if err != nil {
		t.Fatalf("ReadInt8 failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadInt8() = %v, want %v", decoded, value)
	}
}

func TestInt8_DecodeInsufficientData(t *testing.T) {
	_, err := DecodeInt8([]byte{})
	if err != ErrInsufficientData {
		t.Errorf("DecodeInt8() error = %v, want %v", err, ErrInsufficientData)
	}
}

// ============================================================================
// INT16 Tests
// ============================================================================

func TestInt16_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int16
	}{
		{"zero", 0},
		{"positive", 1234},
		{"negative", -1234},
		{"max", 32767},
		{"min", -32768},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt16(tt.value)
			decoded, err := DecodeInt16(encoded)
			if err != nil {
				t.Fatalf("DecodeInt16 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeInt16() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestInt16_WriteRead(t *testing.T) {
	value := int16(1234)
	var buf bytes.Buffer

	if err := WriteInt16(&buf, value); err != nil {
		t.Fatalf("WriteInt16 failed: %v", err)
	}

	decoded, err := ReadInt16(&buf)
	if err != nil {
		t.Fatalf("ReadInt16 failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadInt16() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// UINT16 Tests
// ============================================================================

func TestUint16_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value uint16
	}{
		{"zero", 0},
		{"positive", 1234},
		{"max", 65535},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUint16(tt.value)
			decoded, err := DecodeUint16(encoded)
			if err != nil {
				t.Fatalf("DecodeUint16 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeUint16() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

// ============================================================================
// INT32 Tests
// ============================================================================

func TestInt32_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int32
	}{
		{"zero", 0},
		{"positive", 123456},
		{"negative", -123456},
		{"max", 2147483647},
		{"min", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt32(tt.value)
			decoded, err := DecodeInt32(encoded)
			if err != nil {
				t.Fatalf("DecodeInt32 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeInt32() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestInt32_WriteRead(t *testing.T) {
	value := int32(123456)
	var buf bytes.Buffer

	if err := WriteInt32(&buf, value); err != nil {
		t.Fatalf("WriteInt32 failed: %v", err)
	}

	decoded, err := ReadInt32(&buf)
	if err != nil {
		t.Fatalf("ReadInt32 failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadInt32() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// UINT32 Tests
// ============================================================================

func TestUint32_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value uint32
	}{
		{"zero", 0},
		{"positive", 123456},
		{"max", 4294967295},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUint32(tt.value)
			decoded, err := DecodeUint32(encoded)
			if err != nil {
				t.Fatalf("DecodeUint32 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeUint32() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

// ============================================================================
// INT64 Tests
// ============================================================================

func TestInt64_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"positive", 123456789012345},
		{"negative", -123456789012345},
		{"max", 9223372036854775807},
		{"min", -9223372036854775808},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt64(tt.value)
			decoded, err := DecodeInt64(encoded)
			if err != nil {
				t.Fatalf("DecodeInt64 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeInt64() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestInt64_WriteRead(t *testing.T) {
	value := int64(123456789012345)
	var buf bytes.Buffer

	if err := WriteInt64(&buf, value); err != nil {
		t.Fatalf("WriteInt64 failed: %v", err)
	}

	decoded, err := ReadInt64(&buf)
	if err != nil {
		t.Fatalf("ReadInt64 failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadInt64() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// FLOAT64 Tests
// ============================================================================

func TestFloat64_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value float64
	}{
		{"zero", 0.0},
		{"positive", 123.456},
		{"negative", -123.456},
		{"pi", 3.141592653589793},
		{"max", 1.7976931348623157e+308},
		{"min", -1.7976931348623157e+308},
		{"infinity", math.Inf(1)},
		{"negative infinity", math.Inf(-1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeFloat64(tt.value)
			decoded, err := DecodeFloat64(encoded)
			if err != nil {
				t.Fatalf("DecodeFloat64 failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeFloat64() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestFloat64_WriteRead(t *testing.T) {
	value := 123.456
	var buf bytes.Buffer

	if err := WriteFloat64(&buf, value); err != nil {
		t.Fatalf("WriteFloat64 failed: %v", err)
	}

	decoded, err := ReadFloat64(&buf)
	if err != nil {
		t.Fatalf("ReadFloat64 failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadFloat64() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// STRING Tests
// ============================================================================

func TestString_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"unicode", "Hello, 世界"},
		{"long", "This is a very long string that contains many characters"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeString(tt.value)
			decoded, n, err := DecodeString(encoded)
			if err != nil {
				t.Fatalf("DecodeString failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeString() = %q, want %q", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeString() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestString_WriteRead(t *testing.T) {
	value := "hello, world"
	var buf bytes.Buffer

	if err := WriteString(&buf, value); err != nil {
		t.Fatalf("WriteString failed: %v", err)
	}

	decoded, err := ReadString(&buf)
	if err != nil {
		t.Fatalf("ReadString failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadString() = %q, want %q", decoded, value)
	}
}

// ============================================================================
// NULLABLE_STRING Tests
// ============================================================================

func TestNullableString_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value *string
	}{
		{"nil", nil},
		{"empty", stringPtr("")},
		{"simple", stringPtr("hello")},
		{"unicode", stringPtr("Hello, 世界")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeNullableString(tt.value)
			decoded, n, err := DecodeNullableString(encoded)
			if err != nil {
				t.Fatalf("DecodeNullableString failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeNullableString() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && decoded != nil && *decoded != *tt.value {
				t.Errorf("DecodeNullableString() = %q, want %q", *decoded, *tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeNullableString() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestNullableString_WriteRead(t *testing.T) {
	value := stringPtr("hello")
	var buf bytes.Buffer

	if err := WriteNullableString(&buf, value); err != nil {
		t.Fatalf("WriteNullableString failed: %v", err)
	}

	decoded, err := ReadNullableString(&buf)
	if err != nil {
		t.Fatalf("ReadNullableString failed: %v", err)
	}
	if decoded == nil || *decoded != *value {
		t.Errorf("ReadNullableString() = %v, want %v", decoded, value)
	}
}

func TestNullableString_WriteRead_Nil(t *testing.T) {
	var buf bytes.Buffer

	if err := WriteNullableString(&buf, nil); err != nil {
		t.Fatalf("WriteNullableString failed: %v", err)
	}

	decoded, err := ReadNullableString(&buf)
	if err != nil {
		t.Fatalf("ReadNullableString failed: %v", err)
	}
	if decoded != nil {
		t.Errorf("ReadNullableString() = %v, want nil", decoded)
	}
}

// ============================================================================
// BYTES Tests
// ============================================================================

func TestBytes_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"simple", []byte{1, 2, 3, 4}},
		{"long", bytes.Repeat([]byte{0xFF}, 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeBytes(tt.value)
			decoded, n, err := DecodeBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeBytes failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeBytes() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && !bytes.Equal(decoded, tt.value) {
				t.Errorf("DecodeBytes() = %v, want %v", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeBytes() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestBytes_WriteRead(t *testing.T) {
	value := []byte{1, 2, 3, 4, 5}
	var buf bytes.Buffer

	if err := WriteBytes(&buf, value); err != nil {
		t.Fatalf("WriteBytes failed: %v", err)
	}

	decoded, err := ReadBytes(&buf)
	if err != nil {
		t.Fatalf("ReadBytes failed: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Errorf("ReadBytes() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// NULLABLE_BYTES Tests
// ============================================================================

func TestNullableBytes_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value *[]byte
	}{
		{"nil", nil},
		{"empty", bytesPtr([]byte{})},
		{"simple", bytesPtr([]byte{1, 2, 3})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeNullableBytes(tt.value)
			decoded, n, err := DecodeNullableBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeNullableBytes failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeNullableBytes() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && decoded != nil && !bytes.Equal(*decoded, *tt.value) {
				t.Errorf("DecodeNullableBytes() = %v, want %v", *decoded, *tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeNullableBytes() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// VARINT Tests
// ============================================================================

func TestVarint_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int32
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"small", 127},
		{"medium", 16383},
		{"large", 2097151},
		{"max", 2147483647},
		{"min", -2147483648},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeVarint(tt.value)
			decoded, n, err := DecodeVarint(encoded)
			if err != nil {
				t.Fatalf("DecodeVarint failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeVarint() = %v, want %v", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeVarint() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestVarint_WriteRead(t *testing.T) {
	value := int32(12345)
	var buf bytes.Buffer

	if err := WriteVarint(&buf, value); err != nil {
		t.Fatalf("WriteVarint failed: %v", err)
	}

	decoded, err := ReadVarint(&buf)
	if err != nil {
		t.Fatalf("ReadVarint failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadVarint() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// VARLONG Tests
// ============================================================================

func TestVarlong_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value int64
	}{
		{"zero", 0},
		{"positive", 42},
		{"negative", -42},
		{"small", 127},
		{"medium", 16383},
		{"large", 2097151},
		{"very large", 9223372036854775807},
		{"min", -9223372036854775808},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeVarlong(tt.value)
			decoded, n, err := DecodeVarlong(encoded)
			if err != nil {
				t.Fatalf("DecodeVarlong failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeVarlong() = %v, want %v", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeVarlong() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestVarlong_WriteRead(t *testing.T) {
	value := int64(123456789012345)
	var buf bytes.Buffer

	if err := WriteVarlong(&buf, value); err != nil {
		t.Fatalf("WriteVarlong failed: %v", err)
	}

	decoded, err := ReadVarlong(&buf)
	if err != nil {
		t.Fatalf("ReadVarlong failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadVarlong() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// COMPACT_STRING Tests
// ============================================================================

func TestCompactString_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"empty", ""},
		{"simple", "hello"},
		{"unicode", "Hello, 世界"},
		{"long", "This is a very long string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactString(tt.value)
			decoded, n, err := DecodeCompactString(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactString failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeCompactString() = %q, want %q", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactString() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestCompactString_WriteRead(t *testing.T) {
	value := "hello, world"
	var buf bytes.Buffer

	if err := WriteCompactString(&buf, value); err != nil {
		t.Fatalf("WriteCompactString failed: %v", err)
	}

	decoded, err := ReadCompactString(&buf)
	if err != nil {
		t.Fatalf("ReadCompactString failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadCompactString() = %q, want %q", decoded, value)
	}
}

// ============================================================================
// COMPACT_NULLABLE_STRING Tests
// ============================================================================

func TestCompactNullableString_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value *string
	}{
		{"nil", nil},
		{"empty", stringPtr("")},
		{"simple", stringPtr("hello")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactNullableString(tt.value)
			decoded, n, err := DecodeCompactNullableString(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactNullableString failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactNullableString() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && decoded != nil && *decoded != *tt.value {
				t.Errorf("DecodeCompactNullableString() = %q, want %q", *decoded, *tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactNullableString() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// COMPACT_BYTES Tests
// ============================================================================

func TestCompactBytes_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"simple", []byte{1, 2, 3, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactBytes(tt.value)
			decoded, n, err := DecodeCompactBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactBytes failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactBytes() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && !bytes.Equal(decoded, tt.value) {
				t.Errorf("DecodeCompactBytes() = %v, want %v", decoded, tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactBytes() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// COMPACT_NULLABLE_BYTES Tests
// ============================================================================

func TestCompactNullableBytes_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value *[]byte
	}{
		{"nil", nil},
		{"empty", bytesPtr([]byte{})},
		{"simple", bytesPtr([]byte{1, 2, 3})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactNullableBytes(tt.value)
			decoded, n, err := DecodeCompactNullableBytes(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactNullableBytes failed: %v", err)
			}
			if (tt.value == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactNullableBytes() nil mismatch: got %v, want %v", decoded == nil, tt.value == nil)
			}
			if tt.value != nil && decoded != nil && !bytes.Equal(*decoded, *tt.value) {
				t.Errorf("DecodeCompactNullableBytes() = %v, want %v", *decoded, *tt.value)
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactNullableBytes() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// UUID Tests
// ============================================================================

func TestUUID_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		value uuid.UUID
	}{
		{"nil", uuid.Nil},
		{"random", uuid.Must(uuid.NewRandom())},
		{"specific", uuid.Must(uuid.Parse("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUUID(tt.value)
			decoded, err := DecodeUUID(encoded)
			if err != nil {
				t.Fatalf("DecodeUUID failed: %v", err)
			}
			if decoded != tt.value {
				t.Errorf("DecodeUUID() = %v, want %v", decoded, tt.value)
			}
		})
	}
}

func TestUUID_WriteRead(t *testing.T) {
	value := uuid.Must(uuid.NewRandom())
	var buf bytes.Buffer

	if err := WriteUUID(&buf, value); err != nil {
		t.Fatalf("WriteUUID failed: %v", err)
	}

	decoded, err := ReadUUID(&buf)
	if err != nil {
		t.Fatalf("ReadUUID failed: %v", err)
	}
	if decoded != value {
		t.Errorf("ReadUUID() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// ARRAY Tests
// ============================================================================

func TestArray_EncodeDecode(t *testing.T) {
	// Test with int32 array
	intEncoder := func(item interface{}) ([]byte, error) {
		val, ok := item.(int32)
		if !ok {
			return nil, nil
		}
		return EncodeInt32(val), nil
	}

	intDecoder := func(data []byte) (interface{}, int, error) {
		val, err := DecodeInt32(data)
		if err != nil {
			return nil, 0, err
		}
		return val, 4, nil
	}

	tests := []struct {
		name  string
		items []interface{}
	}{
		{"nil", nil},
		{"empty", []interface{}{}},
		{"single", []interface{}{int32(42)}},
		{"multiple", []interface{}{int32(1), int32(2), int32(3)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeArray(tt.items, intEncoder)
			if err != nil {
				t.Fatalf("EncodeArray failed: %v", err)
			}

			decoded, n, err := DecodeArray(encoded, intDecoder)
			if err != nil {
				t.Fatalf("DecodeArray failed: %v", err)
			}

			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeArray() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeArray() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeArray()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeArray() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// COMPACT_ARRAY Tests
// ============================================================================

func TestCompactArray_EncodeDecode(t *testing.T) {
	// Test with string array
	strEncoder := func(item interface{}) ([]byte, error) {
		val, ok := item.(string)
		if !ok {
			return nil, nil
		}
		return EncodeString(val), nil
	}

	strDecoder := func(data []byte) (interface{}, int, error) {
		val, n, err := DecodeString(data)
		if err != nil {
			return nil, 0, err
		}
		return val, n, nil
	}

	tests := []struct {
		name  string
		items []interface{}
	}{
		{"nil", nil},
		{"empty", []interface{}{}},
		{"single", []interface{}{"hello"}},
		{"multiple", []interface{}{"a", "b", "c"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeCompactArray(tt.items, strEncoder)
			if err != nil {
				t.Fatalf("EncodeCompactArray failed: %v", err)
			}

			decoded, n, err := DecodeCompactArray(encoded, strDecoder)
			if err != nil {
				t.Fatalf("DecodeCompactArray failed: %v", err)
			}

			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactArray() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeCompactArray() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeCompactArray()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactArray() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// RECORDS Tests
// ============================================================================

func TestRecords_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		records []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"simple", []byte{1, 2, 3, 4}},
		{"long", bytes.Repeat([]byte{0xFF}, 100)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeRecords(tt.records)
			decoded, n, err := DecodeRecords(encoded)
			if err != nil {
				t.Fatalf("DecodeRecords failed: %v", err)
			}
			if (tt.records == nil) != (decoded == nil) {
				t.Errorf("DecodeRecords() nil mismatch: got %v, want %v", decoded == nil, tt.records == nil)
			}
			if tt.records != nil && !bytes.Equal(decoded, tt.records) {
				t.Errorf("DecodeRecords() = %v, want %v", decoded, tt.records)
			}
			if n != len(encoded) {
				t.Errorf("DecodeRecords() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestRecords_WriteRead(t *testing.T) {
	value := []byte{1, 2, 3, 4, 5}
	var buf bytes.Buffer

	if err := WriteRecords(&buf, value); err != nil {
		t.Fatalf("WriteRecords failed: %v", err)
	}

	decoded, err := ReadRecords(&buf)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	}
	if !bytes.Equal(decoded, value) {
		t.Errorf("ReadRecords() = %v, want %v", decoded, value)
	}
}

// ============================================================================
// COMPACT_RECORDS Tests
// ============================================================================

func TestCompactRecords_EncodeDecode(t *testing.T) {
	tests := []struct {
		name    string
		records []byte
	}{
		{"nil", nil},
		{"empty", []byte{}},
		{"simple", []byte{1, 2, 3, 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactRecords(tt.records)
			decoded, n, err := DecodeCompactRecords(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactRecords failed: %v", err)
			}
			if (tt.records == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactRecords() nil mismatch: got %v, want %v", decoded == nil, tt.records == nil)
			}
			if tt.records != nil && !bytes.Equal(decoded, tt.records) {
				t.Errorf("DecodeCompactRecords() = %v, want %v", decoded, tt.records)
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactRecords() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// Typed Array Helper Tests
// ============================================================================

func TestInt8Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []int8
	}{
		{"nil", nil},
		{"empty", []int8{}},
		{"single", []int8{42}},
		{"multiple", []int8{1, 2, 3, -1, 127, -128}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt8Array(tt.items)
			decoded, n, err := DecodeInt8Array(encoded)
			if err != nil {
				t.Fatalf("DecodeInt8Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeInt8Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeInt8Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeInt8Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeInt8Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestInt8Array_WriteRead(t *testing.T) {
	items := []int8{1, 2, 3, -1, 127}
	var buf bytes.Buffer

	if err := WriteInt8Array(&buf, items); err != nil {
		t.Fatalf("WriteInt8Array failed: %v", err)
	}

	decoded, err := ReadInt8Array(&buf)
	if err != nil {
		t.Fatalf("ReadInt8Array failed: %v", err)
	}
	if !equalInt8Slices(decoded, items) {
		t.Errorf("ReadInt8Array() = %v, want %v", decoded, items)
	}
}

func TestInt16Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []int16
	}{
		{"nil", nil},
		{"empty", []int16{}},
		{"single", []int16{42}},
		{"multiple", []int16{1, 2, 3, -1, 32767, -32768}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt16Array(tt.items)
			decoded, n, err := DecodeInt16Array(encoded)
			if err != nil {
				t.Fatalf("DecodeInt16Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeInt16Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeInt16Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeInt16Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeInt16Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestInt32Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []int32
	}{
		{"nil", nil},
		{"empty", []int32{}},
		{"single", []int32{42}},
		{"multiple", []int32{1, 2, 3, -1, 2147483647, -2147483648}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt32Array(tt.items)
			decoded, n, err := DecodeInt32Array(encoded)
			if err != nil {
				t.Fatalf("DecodeInt32Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeInt32Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeInt32Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeInt32Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeInt32Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestInt32Array_WriteRead(t *testing.T) {
	items := []int32{1, 2, 3, -1, 2147483647}
	var buf bytes.Buffer

	if err := WriteInt32Array(&buf, items); err != nil {
		t.Fatalf("WriteInt32Array failed: %v", err)
	}

	decoded, err := ReadInt32Array(&buf)
	if err != nil {
		t.Fatalf("ReadInt32Array failed: %v", err)
	}
	if !equalInt32Slices(decoded, items) {
		t.Errorf("ReadInt32Array() = %v, want %v", decoded, items)
	}
}

func TestInt64Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []int64
	}{
		{"nil", nil},
		{"empty", []int64{}},
		{"single", []int64{42}},
		{"multiple", []int64{1, 2, 3, -1, 9223372036854775807, -9223372036854775808}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeInt64Array(tt.items)
			decoded, n, err := DecodeInt64Array(encoded)
			if err != nil {
				t.Fatalf("DecodeInt64Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeInt64Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeInt64Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeInt64Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeInt64Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestUint16Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []uint16
	}{
		{"nil", nil},
		{"empty", []uint16{}},
		{"single", []uint16{42}},
		{"multiple", []uint16{1, 2, 3, 65535}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUint16Array(tt.items)
			decoded, n, err := DecodeUint16Array(encoded)
			if err != nil {
				t.Fatalf("DecodeUint16Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeUint16Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeUint16Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeUint16Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeUint16Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestUint32Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []uint32
	}{
		{"nil", nil},
		{"empty", []uint32{}},
		{"single", []uint32{42}},
		{"multiple", []uint32{1, 2, 3, 4294967295}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUint32Array(tt.items)
			decoded, n, err := DecodeUint32Array(encoded)
			if err != nil {
				t.Fatalf("DecodeUint32Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeUint32Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeUint32Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeUint32Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeUint32Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestStringArray_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []string
	}{
		{"nil", nil},
		{"empty", []string{}},
		{"single", []string{"hello"}},
		{"multiple", []string{"a", "b", "c", "Hello, 世界"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeStringArray(tt.items)
			decoded, n, err := DecodeStringArray(encoded)
			if err != nil {
				t.Fatalf("DecodeStringArray failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeStringArray() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeStringArray() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeStringArray()[%d] = %q, want %q", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeStringArray() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestStringArray_WriteRead(t *testing.T) {
	items := []string{"hello", "world", "test"}
	var buf bytes.Buffer

	if err := WriteStringArray(&buf, items); err != nil {
		t.Fatalf("WriteStringArray failed: %v", err)
	}

	decoded, err := ReadStringArray(&buf)
	if err != nil {
		t.Fatalf("ReadStringArray failed: %v", err)
	}
	if !equalStringSlices(decoded, items) {
		t.Errorf("ReadStringArray() = %v, want %v", decoded, items)
	}
}

func TestFloat64Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []float64
	}{
		{"nil", nil},
		{"empty", []float64{}},
		{"single", []float64{42.5}},
		{"multiple", []float64{1.1, 2.2, 3.3, -1.5, math.Pi}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeFloat64Array(tt.items)
			decoded, n, err := DecodeFloat64Array(encoded)
			if err != nil {
				t.Fatalf("DecodeFloat64Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeFloat64Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeFloat64Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeFloat64Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeFloat64Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestUUIDArray_EncodeDecode(t *testing.T) {
	uuid1 := uuid.Must(uuid.NewRandom())
	uuid2 := uuid.Must(uuid.NewRandom())
	uuid3 := uuid.Nil

	tests := []struct {
		name  string
		items []uuid.UUID
	}{
		{"nil", nil},
		{"empty", []uuid.UUID{}},
		{"single", []uuid.UUID{uuid1}},
		{"multiple", []uuid.UUID{uuid1, uuid2, uuid3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeUUIDArray(tt.items)
			decoded, n, err := DecodeUUIDArray(encoded)
			if err != nil {
				t.Fatalf("DecodeUUIDArray failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeUUIDArray() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeUUIDArray() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeUUIDArray()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeUUIDArray() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

// ============================================================================
// Typed Compact Array Helper Tests
// ============================================================================

func TestCompactInt32Array_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []int32
	}{
		{"nil", nil},
		{"empty", []int32{}},
		{"single", []int32{42}},
		{"multiple", []int32{1, 2, 3, -1, 2147483647}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactInt32Array(tt.items)
			decoded, n, err := DecodeCompactInt32Array(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactInt32Array failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactInt32Array() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeCompactInt32Array() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeCompactInt32Array()[%d] = %v, want %v", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactInt32Array() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestCompactInt32Array_WriteRead(t *testing.T) {
	items := []int32{1, 2, 3, -1, 2147483647}
	var buf bytes.Buffer

	if err := WriteCompactInt32Array(&buf, items); err != nil {
		t.Fatalf("WriteCompactInt32Array failed: %v", err)
	}

	decoded, err := ReadCompactInt32Array(&buf)
	if err != nil {
		t.Fatalf("ReadCompactInt32Array failed: %v", err)
	}
	if !equalInt32Slices(decoded, items) {
		t.Errorf("ReadCompactInt32Array() = %v, want %v", decoded, items)
	}
}

func TestCompactStringArray_EncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		items []string
	}{
		{"nil", nil},
		{"empty", []string{}},
		{"single", []string{"hello"}},
		{"multiple", []string{"a", "b", "c", "Hello, 世界"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeCompactStringArray(tt.items)
			decoded, n, err := DecodeCompactStringArray(encoded)
			if err != nil {
				t.Fatalf("DecodeCompactStringArray failed: %v", err)
			}
			if (tt.items == nil) != (decoded == nil) {
				t.Errorf("DecodeCompactStringArray() nil mismatch: got %v, want %v", decoded == nil, tt.items == nil)
			}
			if tt.items != nil {
				if len(decoded) != len(tt.items) {
					t.Errorf("DecodeCompactStringArray() length = %d, want %d", len(decoded), len(tt.items))
				}
				for i, item := range tt.items {
					if decoded[i] != item {
						t.Errorf("DecodeCompactStringArray()[%d] = %q, want %q", i, decoded[i], item)
					}
				}
			}
			if n != len(encoded) {
				t.Errorf("DecodeCompactStringArray() consumed %d bytes, want %d", n, len(encoded))
			}
		})
	}
}

func TestCompactStringArray_WriteRead(t *testing.T) {
	items := []string{"hello", "world", "test"}
	var buf bytes.Buffer

	if err := WriteCompactStringArray(&buf, items); err != nil {
		t.Fatalf("WriteCompactStringArray failed: %v", err)
	}

	decoded, err := ReadCompactStringArray(&buf)
	if err != nil {
		t.Fatalf("ReadCompactStringArray failed: %v", err)
	}
	if !equalStringSlices(decoded, items) {
		t.Errorf("ReadCompactStringArray() = %v, want %v", decoded, items)
	}
}

// ============================================================================
// Helper functions
// ============================================================================

func stringPtr(s string) *string {
	return &s
}

func bytesPtr(b []byte) *[]byte {
	return &b
}

func equalInt8Slices(a, b []int8) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalInt32Slices(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
