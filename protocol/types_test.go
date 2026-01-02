package protocol

import (
	"bytes"
	"testing"

	"github.com/google/uuid"
)

func TestBool(t *testing.T) {
	tests := []bool{true, false}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteBool(&buf, original)
		if err != nil {
			t.Fatalf("WriteBool failed: %v", err)
		}

		result, err := ReadBool(&buf)
		if err != nil {
			t.Fatalf("ReadBool failed: %v", err)
		}

		if result != original {
			t.Errorf("Bool mismatch: expected %v, got %v", original, result)
		}
	}
}

func TestInt8(t *testing.T) {
	tests := []int8{-128, -1, 0, 1, 127}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteInt8(&buf, original)
		if err != nil {
			t.Fatalf("WriteInt8 failed: %v", err)
		}

		result, err := ReadInt8(&buf)
		if err != nil {
			t.Fatalf("ReadInt8 failed: %v", err)
		}

		if result != original {
			t.Errorf("Int8 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestInt16(t *testing.T) {
	tests := []int16{-32768, -1, 0, 1, 32767}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteInt16(&buf, original)
		if err != nil {
			t.Fatalf("WriteInt16 failed: %v", err)
		}

		result, err := ReadInt16(&buf)
		if err != nil {
			t.Fatalf("ReadInt16 failed: %v", err)
		}

		if result != original {
			t.Errorf("Int16 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestInt32(t *testing.T) {
	tests := []int32{-2147483648, -1, 0, 1, 2147483647}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteInt32(&buf, original)
		if err != nil {
			t.Fatalf("WriteInt32 failed: %v", err)
		}

		result, err := ReadInt32(&buf)
		if err != nil {
			t.Fatalf("ReadInt32 failed: %v", err)
		}

		if result != original {
			t.Errorf("Int32 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestInt64(t *testing.T) {
	tests := []int64{-9223372036854775808, -1, 0, 1, 9223372036854775807}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteInt64(&buf, original)
		if err != nil {
			t.Fatalf("WriteInt64 failed: %v", err)
		}

		result, err := ReadInt64(&buf)
		if err != nil {
			t.Fatalf("ReadInt64 failed: %v", err)
		}

		if result != original {
			t.Errorf("Int64 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestUint16(t *testing.T) {
	tests := []uint16{0, 1, 65535}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteUint16(&buf, original)
		if err != nil {
			t.Fatalf("WriteUint16 failed: %v", err)
		}

		result, err := ReadUInt16(&buf)
		if err != nil {
			t.Fatalf("ReadUInt16 failed: %v", err)
		}

		if result != original {
			t.Errorf("Uint16 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestUint32(t *testing.T) {
	tests := []uint32{0, 1, 4294967295}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteUint32(&buf, original)
		if err != nil {
			t.Fatalf("WriteUint32 failed: %v", err)
		}

		result, err := ReadUInt32(&buf)
		if err != nil {
			t.Fatalf("ReadUInt32 failed: %v", err)
		}

		if result != original {
			t.Errorf("Uint32 mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestVarint(t *testing.T) {
	tests := []int64{-2147483648, -1, 0, 1, 2147483647}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteVarint(&buf, original)
		if err != nil {
			t.Fatalf("WriteVarint failed: %v", err)
		}

		result, err := ReadVarint(&buf)
		if err != nil {
			t.Fatalf("ReadVarint failed: %v", err)
		}

		if result != original {
			t.Errorf("Varint mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestUvarint(t *testing.T) {
	tests := []uint64{0, 1, 4294967295}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteUvarint(&buf, original)
		if err != nil {
			t.Fatalf("WriteUvarint failed: %v", err)
		}

		result, err := ReadUvarint(&buf)
		if err != nil {
			t.Fatalf("ReadUvarint failed: %v", err)
		}

		if result != original {
			t.Errorf("Uvarint mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestVarlong(t *testing.T) {
	tests := []int64{-9223372036854775808, -1, 0, 1, 9223372036854775807}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteVarlong(&buf, original)
		if err != nil {
			t.Fatalf("WriteVarlong failed: %v", err)
		}

		result, err := ReadVarlong(&buf)
		if err != nil {
			t.Fatalf("ReadVarlong failed: %v", err)
		}

		if result != original {
			t.Errorf("Varlong mismatch: expected %d, got %d", original, result)
		}
	}
}

func TestUUID(t *testing.T) {
	original := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	var buf bytes.Buffer
	err := WriteUUID(&buf, original)
	if err != nil {
		t.Fatalf("WriteUUID failed: %v", err)
	}

	result, err := ReadUUID(&buf)
	if err != nil {
		t.Fatalf("ReadUUID failed: %v", err)
	}

	if result != original {
		t.Errorf("UUID mismatch: expected %v, got %v", original, result)
	}
}

func TestFloat64(t *testing.T) {
	tests := []float64{-123.456, 0.0, 123.456, 3.141592653589793}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteFloat64(&buf, original)
		if err != nil {
			t.Fatalf("WriteFloat64 failed: %v", err)
		}

		result, err := ReadFloat64(&buf)
		if err != nil {
			t.Fatalf("ReadFloat64 failed: %v", err)
		}

		if result != original {
			t.Errorf("Float64 mismatch: expected %f, got %f", original, result)
		}
	}
}

func TestString(t *testing.T) {
	tests := []string{"", "hello", "world", "test string with spaces"}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteString(&buf, original)
		if err != nil {
			t.Fatalf("WriteString failed: %v", err)
		}

		result, err := ReadString(&buf)
		if err != nil {
			t.Fatalf("ReadString failed: %v", err)
		}

		if result != original {
			t.Errorf("String mismatch: expected %q, got %q", original, result)
		}
	}
}

func TestNullableString(t *testing.T) {
	tests := []struct {
		name     string
		original *string
	}{
		{"nil", nil},
		{"empty", stringPtr("")},
		{"non-empty", stringPtr("hello world")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableString(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableString failed: %v", err)
			}

			result, err := ReadNullableString(&buf)
			if err != nil {
				t.Fatalf("ReadNullableString failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableString mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableString mismatch: expected %q, got nil", *tt.original)
				} else if *result != *tt.original {
					t.Errorf("NullableString mismatch: expected %q, got %q", *tt.original, *result)
				}
			}
		})
	}
}

func TestCompactString(t *testing.T) {
	tests := []string{"", "hello", "world", "test string with spaces"}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteCompactString(&buf, original)
		if err != nil {
			t.Fatalf("WriteCompactString failed: %v", err)
		}

		result, err := ReadCompactString(&buf)
		if err != nil {
			t.Fatalf("ReadCompactString failed: %v", err)
		}

		if result != original {
			t.Errorf("CompactString mismatch: expected %q, got %q", original, result)
		}
	}
}

func TestNullableCompactString(t *testing.T) {
	tests := []struct {
		name     string
		original *string
	}{
		{"nil", nil},
		{"empty", stringPtr("")},
		{"non-empty", stringPtr("hello world")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableCompactString(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableCompactString failed: %v", err)
			}

			result, err := ReadNullableCompactString(&buf)
			if err != nil {
				t.Fatalf("ReadNullableCompactString failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableCompactString mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableCompactString mismatch: expected %q, got nil", *tt.original)
				} else if *result != *tt.original {
					t.Errorf("NullableCompactString mismatch: expected %q, got %q", *tt.original, *result)
				}
			}
		})
	}
}

func TestBytes(t *testing.T) {
	tests := [][]byte{
		{},
		{0x00},
		{0x01, 0x02, 0x03},
		[]byte("hello world"),
	}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteBytes(&buf, original)
		if err != nil {
			t.Fatalf("WriteBytes failed: %v", err)
		}

		result, err := ReadBytes(&buf)
		if err != nil {
			t.Fatalf("ReadBytes failed: %v", err)
		}

		if !bytes.Equal(result, original) {
			t.Errorf("Bytes mismatch: expected %v, got %v", original, result)
		}
	}
}

func TestCompactBytes(t *testing.T) {
	tests := [][]byte{
		{},
		{0x00},
		{0x01, 0x02, 0x03},
		[]byte("hello world"),
	}

	for _, original := range tests {
		var buf bytes.Buffer
		err := WriteCompactBytes(&buf, original)
		if err != nil {
			t.Fatalf("WriteCompactBytes failed: %v", err)
		}

		result, err := ReadCompactBytes(&buf)
		if err != nil {
			t.Fatalf("ReadCompactBytes failed: %v", err)
		}

		if !bytes.Equal(result, original) {
			t.Errorf("CompactBytes mismatch: expected %v, got %v", original, result)
		}
	}
}

func TestNullableBytes(t *testing.T) {
	tests := []struct {
		name     string
		original *[]byte
	}{
		{"nil", nil},
		{"empty", byteSlicePtr([]byte{})},
		{"non-empty", byteSlicePtr([]byte{0x01, 0x02, 0x03})},
		{"string bytes", byteSlicePtr([]byte("hello world"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableBytes(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableBytes failed: %v", err)
			}

			result, err := ReadNullableBytes(&buf)
			if err != nil {
				t.Fatalf("ReadNullableBytes failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableBytes mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableBytes mismatch: expected %v, got nil", *tt.original)
				} else if !bytes.Equal(*result, *tt.original) {
					t.Errorf("NullableBytes mismatch: expected %v, got %v", *tt.original, *result)
				}
			}
		})
	}
}

func TestNullableCompactBytes(t *testing.T) {
	tests := []struct {
		name     string
		original *[]byte
	}{
		{"nil", nil},
		{"empty", byteSlicePtr([]byte{})},
		{"non-empty", byteSlicePtr([]byte{0x01, 0x02, 0x03})},
		{"string bytes", byteSlicePtr([]byte("hello world"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableCompactBytes(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableCompactBytes failed: %v", err)
			}

			result, err := ReadNullableCompactBytes(&buf)
			if err != nil {
				t.Fatalf("ReadNullableCompactBytes failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableCompactBytes mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableCompactBytes mismatch: expected %v, got nil", *tt.original)
				} else if !bytes.Equal(*result, *tt.original) {
					t.Errorf("NullableCompactBytes mismatch: expected %v, got %v", *tt.original, *result)
				}
			}
		})
	}
}

func TestRecords(t *testing.T) {
	tests := []struct {
		name     string
		original *[]byte
	}{
		{"nil", nil},
		{"empty", byteSlicePtr([]byte{})},
		{"non-empty", byteSlicePtr([]byte{0x01, 0x02, 0x03})},
		{"string bytes", byteSlicePtr([]byte("hello world"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteRecords(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteRecords failed: %v", err)
			}

			result, err := ReadRecords(&buf)
			if err != nil {
				t.Fatalf("ReadRecords failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("Records mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("Records mismatch: expected %v, got nil", *tt.original)
				} else if !bytes.Equal(*result, *tt.original) {
					t.Errorf("Records mismatch: expected %v, got %v", *tt.original, *result)
				}
			}
		})
	}
}

func TestCompactRecords(t *testing.T) {
	tests := []struct {
		name     string
		original *[]byte
	}{
		{"nil", nil},
		{"empty", byteSlicePtr([]byte{})},
		{"non-empty", byteSlicePtr([]byte{0x01, 0x02, 0x03})},
		{"string bytes", byteSlicePtr([]byte("hello world"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteCompactRecords(&buf, tt.original)
			if err != nil {
				t.Fatalf("WriteCompactRecords failed: %v", err)
			}

			result, err := ReadCompactRecords(&buf)
			if err != nil {
				t.Fatalf("ReadCompactRecords failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("CompactRecords mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("CompactRecords mismatch: expected %v, got nil", *tt.original)
				} else if !bytes.Equal(*result, *tt.original) {
					t.Errorf("CompactRecords mismatch: expected %v, got %v", *tt.original, *result)
				}
			}
		})
	}
}

func TestArray(t *testing.T) {
	// Test with int32 array
	original := []int32{1, 2, 3, 4, 5}

	var buf bytes.Buffer
	err := WriteArray(&buf, WriteInt32, original)
	if err != nil {
		t.Fatalf("WriteArray failed: %v", err)
	}

	result, err := ReadArray(&buf, ReadInt32)
	if err != nil {
		t.Fatalf("ReadArray failed: %v", err)
	}

	if len(result) != len(original) {
		t.Fatalf("Array length mismatch: expected %d, got %d", len(original), len(result))
	}

	for i := range original {
		if result[i] != original[i] {
			t.Errorf("Array[%d] mismatch: expected %d, got %d", i, original[i], result[i])
		}
	}
}

func TestArrayEmpty(t *testing.T) {
	// Test with empty array
	original := []int32{}

	var buf bytes.Buffer
	err := WriteArray(&buf, WriteInt32, original)
	if err != nil {
		t.Fatalf("WriteArray failed: %v", err)
	}

	result, err := ReadArray(&buf, ReadInt32)
	if err != nil {
		t.Fatalf("ReadArray failed: %v", err)
	}

	if len(result) != 0 {
		t.Fatalf("Array length mismatch: expected 0, got %d", len(result))
	}
}

func TestNullableArray(t *testing.T) {
	tests := []struct {
		name     string
		original *[]int32
	}{
		{"nil", nil},
		{"empty", int32SlicePtr([]int32{})},
		{"non-empty", int32SlicePtr([]int32{1, 2, 3})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableArray(&buf, WriteInt32, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableArray failed: %v", err)
			}

			result, err := ReadNullableArray(&buf, ReadInt32)
			if err != nil {
				t.Fatalf("ReadNullableArray failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableArray mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableArray mismatch: expected %v, got nil", *tt.original)
				} else if len(*result) != len(*tt.original) {
					t.Errorf("NullableArray length mismatch: expected %d, got %d", len(*tt.original), len(*result))
				} else {
					for i := range *tt.original {
						if (*result)[i] != (*tt.original)[i] {
							t.Errorf("NullableArray[%d] mismatch: expected %d, got %d", i, (*tt.original)[i], (*result)[i])
						}
					}
				}
			}
		})
	}
}

func TestNullableCompactArray(t *testing.T) {
	tests := []struct {
		name     string
		original *[]int32
	}{
		{"nil", nil},
		{"empty", int32SlicePtr([]int32{})},
		{"non-empty", int32SlicePtr([]int32{1, 2, 3})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := WriteNullableCompactArray(&buf, WriteInt32, tt.original)
			if err != nil {
				t.Fatalf("WriteNullableCompactArray failed: %v", err)
			}

			result, err := ReadNullableCompactArray(&buf, ReadInt32)
			if err != nil {
				t.Fatalf("ReadNullableCompactArray failed: %v", err)
			}

			if tt.original == nil {
				if result != nil {
					t.Errorf("NullableCompactArray mismatch: expected nil, got %v", result)
				}
			} else {
				if result == nil {
					t.Errorf("NullableCompactArray mismatch: expected %v, got nil", *tt.original)
				} else if len(*result) != len(*tt.original) {
					t.Errorf("NullableCompactArray length mismatch: expected %d, got %d", len(*tt.original), len(*result))
				} else {
					for i := range *tt.original {
						if (*result)[i] != (*tt.original)[i] {
							t.Errorf("NullableCompactArray[%d] mismatch: expected %d, got %d", i, (*tt.original)[i], (*result)[i])
						}
					}
				}
			}
		})
	}
}

// Helper functions
func stringPtr(s string) *string {
	return &s
}

func byteSlicePtr(b []byte) *[]byte {
	return &b
}

func int32SlicePtr(i []int32) *[]int32 {
	return &i
}

