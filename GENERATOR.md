# Kafka Protocol Generator

This generator reads JSON definition files from the `sources/` directory and generates Go code for Kafka API requests and responses.

## Current Status

### ✅ Completed Features

1. **JSON Parsing**: Parses Kafka protocol JSON files (with comment removal)
2. **Struct Generation**: Generates Go structs for requests/responses
3. **Version Handling**: Respects version ranges and field versioning
4. **Flexible Versions**: Uses compact types (COMPACT_STRING, COMPACT_BYTES) for flexible versions
5. **Basic Encoding/Decoding**: Generates Encode, Decode, Write, and Read methods
6. **Header Versions**: Includes API key and header version constants
7. **Primitive Types**: Supports bool, int8, int16, int32, int64, string, bytes, uuid, records

### ⚠️ Partially Implemented

1. **Arrays**: Basic structure generated, but encoding/decoding needs completion
2. **Nested Structs**: Structure generated, but encoding/decoding needs completion
3. **Tagged Fields**: Referenced in code but methods not yet implemented
4. **Nullable Fields**: Supported but may need edge case handling

### ❌ Not Yet Implemented

1. **Complete Array Encoding/Decoding**: Need to handle different element types
2. **Complete Struct Encoding/Decoding**: Need recursive handling of nested structures
3. **Tagged Fields Implementation**: Need writeTaggedFields and readTaggedFields methods
4. **Default Values**: Need to handle default field values properly
5. **Map Types**: If any fields use map types

## Usage

```bash
go run ./cmd/generate sources api
```

Or build and run:
```bash
go build ./cmd/generate
./generate sources api
```

## Generated Code Structure

Each request/response gets its own file in the `api/` directory:
- `{name}.go` - Contains struct definition and encoding/decoding methods

### Example Generated Code

```go
type ApiVersionsRequest struct {
    ClientSoftwareName    string `json:"clientsoftwarename" versions:"3-999"`
    ClientSoftwareVersion string `json:"clientsoftwareversion" versions:"3-999"`
}

func (m *ApiVersionsRequest) Write(w io.Writer, version int16) error {
    // Version checking
    // Field encoding based on version
    // Tagged fields if flexible
}
```

## Next Steps

To complete the generator:

1. **Implement Array Encoding/Decoding**: 
   - Handle different element types (primitives, structs, strings)
   - Use compact arrays for flexible versions
   - Handle nullable arrays

2. **Implement Struct Encoding/Decoding**:
   - Recursive encoding/decoding of nested structures
   - Handle versioning within nested structs

3. **Implement Tagged Fields**:
   - Generate `writeTaggedFields` method
   - Generate `readTaggedFields` method
   - Handle tag numbers and version ranges

4. **Fix JSON Parsing**:
   - Better handling of `default` field (can be string, number, or bool)
   - Better handling of `ignorable` field (can be string or bool)

5. **Add Default Value Handling**:
   - Initialize fields with default values when reading
   - Skip writing fields with default values for tagged fields

## Testing

After generation, test the code:
```bash
go build ./api
go test ./api
```

