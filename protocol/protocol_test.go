package protocol

import (
	"bytes"
	"io"
	"testing"
)

func TestRequestWrite(t *testing.T) {
	tests := []struct {
		name         string
		request      Request
		expectTagged bool
	}{
		{
			name: "request without tagged fields (old version)",
			request: Request{
				RequestHeader: RequestHeader{
					ApiKey:        0, // Produce
					ApiVersion:    0, // Version 0 doesn't have tagged fields
					CorrelationId: 123,
					ClientId:      stringPtr("test-client"),
				},
				Body: bytes.NewBuffer([]byte("body data")),
			},
			expectTagged: false,
		},
		{
			name: "request with tagged fields (new version)",
			request: Request{
				RequestHeader: RequestHeader{
					ApiKey:        0, // Produce
					ApiVersion:    9, // Version 9 has tagged fields
					CorrelationId: 456,
					ClientId:      stringPtr("test-client-v2"),
				},
				Body: bytes.NewBuffer([]byte("body data v2")),
			},
			expectTagged: true,
		},
		{
			name: "request with nil client ID",
			request: Request{
				RequestHeader: RequestHeader{
					ApiKey:        1, // Fetch
					ApiVersion:    0,
					CorrelationId: 789,
					ClientId:      nil,
				},
				Body: bytes.NewBuffer([]byte("body")),
			},
			expectTagged: false,
		},
		{
			name: "request with empty body",
			request: Request{
				RequestHeader: RequestHeader{
					ApiKey:        3, // Metadata
					ApiVersion:    0,
					CorrelationId: 999,
					ClientId:      stringPtr("client"),
				},
				Body: bytes.NewBuffer([]byte{}),
			},
			expectTagged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Preserve original body for comparison
			originalBody := make([]byte, tt.request.Body.Len())
			copy(originalBody, tt.request.Body.Bytes())

			var buf bytes.Buffer
			err := tt.request.Write(&buf)
			if err != nil {
				t.Fatalf("Request.Write failed: %v", err)
			}

			// Verify we can read it back
			readRequest, err := ReadRequest(&buf)
			if err != nil {
				t.Fatalf("ReadRequest failed: %v", err)
			}

			if readRequest.ApiKey != tt.request.ApiKey {
				t.Errorf("ApiKey mismatch: expected %d, got %d", tt.request.ApiKey, readRequest.ApiKey)
			}

			if readRequest.ApiVersion != tt.request.ApiVersion {
				t.Errorf("ApiVersion mismatch: expected %d, got %d", tt.request.ApiVersion, readRequest.ApiVersion)
			}

			if readRequest.CorrelationId != tt.request.CorrelationId {
				t.Errorf("CorrelationId mismatch: expected %d, got %d", tt.request.CorrelationId, readRequest.CorrelationId)
			}

			if tt.request.ClientId == nil {
				if readRequest.ClientId != nil {
					t.Errorf("ClientId mismatch: expected nil, got %v", readRequest.ClientId)
				}
			} else {
				if readRequest.ClientId == nil {
					t.Errorf("ClientId mismatch: expected %v, got nil", *tt.request.ClientId)
				} else if *readRequest.ClientId != *tt.request.ClientId {
					t.Errorf("ClientId mismatch: expected %v, got %v", *tt.request.ClientId, *readRequest.ClientId)
				}
			}

			if !bytes.Equal(readRequest.Body.Bytes(), originalBody) {
				t.Errorf("Body mismatch: expected %v, got %v", originalBody, readRequest.Body.Bytes())
			}
		})
	}
}

func TestReadRequest(t *testing.T) {
	// Test reading a complete request
	originalBody := []byte("test body data")
	original := Request{
		RequestHeader: RequestHeader{
			ApiKey:        0,
			ApiVersion:    0,
			CorrelationId: 12345,
			ClientId:      stringPtr("test-client"),
		},
		Body: bytes.NewBuffer(originalBody),
	}

	var buf bytes.Buffer
	err := original.Write(&buf)
	if err != nil {
		t.Fatalf("Request.Write failed: %v", err)
	}

	readRequest, err := ReadRequest(&buf)
	if err != nil {
		t.Fatalf("ReadRequest failed: %v", err)
	}

	if readRequest.ApiKey != original.ApiKey {
		t.Errorf("ApiKey mismatch: expected %d, got %d", original.ApiKey, readRequest.ApiKey)
	}

	if readRequest.ApiVersion != original.ApiVersion {
		t.Errorf("ApiVersion mismatch: expected %d, got %d", original.ApiVersion, readRequest.ApiVersion)
	}

	if readRequest.CorrelationId != original.CorrelationId {
		t.Errorf("CorrelationId mismatch: expected %d, got %d", original.CorrelationId, readRequest.CorrelationId)
	}

	if original.ClientId != nil {
		if readRequest.ClientId == nil {
			t.Errorf("ClientId mismatch: expected %v, got nil", *original.ClientId)
		} else if *readRequest.ClientId != *original.ClientId {
			t.Errorf("ClientId mismatch: expected %v, got %v", *original.ClientId, *readRequest.ClientId)
		}
	}

	if !bytes.Equal(readRequest.Body.Bytes(), originalBody) {
		t.Errorf("Body mismatch: expected %v, got %v", originalBody, readRequest.Body.Bytes())
	}
}

func TestReadRequestEOF(t *testing.T) {
	// Test reading from empty reader
	emptyReader := bytes.NewReader([]byte{})
	_, err := ReadRequest(emptyReader)
	if err != io.EOF {
		t.Errorf("Expected io.EOF for empty reader, got: %v", err)
	}
}

func TestResponseWrite(t *testing.T) {
	tests := []struct {
		name         string
		response     Response
		expectTagged bool
	}{
		{
			name: "response without tagged fields (old version)",
			response: Response{
				ResponseHeader: ResponseHeader{
					ApiKey:        0, // Produce
					ApiVersion:    0, // Version 0 doesn't have tagged fields
					CorrelationId: 123,
					ClientId:      stringPtr("test-client"),
				},
				Body: bytes.NewBuffer([]byte("response body")),
			},
			expectTagged: false,
		},
		{
			name: "response with tagged fields (new version)",
			response: Response{
				ResponseHeader: ResponseHeader{
					ApiKey:        0, // Produce
					ApiVersion:    9, // Version 9 has tagged fields
					CorrelationId: 456,
					ClientId:      stringPtr("test-client-v2"),
				},
				Body: bytes.NewBuffer([]byte("response body v2")),
			},
			expectTagged: true,
		},
		{
			name: "response with empty body",
			response: Response{
				ResponseHeader: ResponseHeader{
					ApiKey:        1, // Fetch
					ApiVersion:    0,
					CorrelationId: 789,
					ClientId:      stringPtr("client"),
				},
				Body: bytes.NewBuffer([]byte{}),
			},
			expectTagged: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := tt.response.Write(&buf)
			if err != nil {
				t.Fatalf("Response.Write failed: %v", err)
			}

			// Verify the response was written correctly by checking size
			if buf.Len() == 0 {
				t.Error("Response.Write produced empty buffer")
			}
		})
	}
}

func TestReadResponse(t *testing.T) {
	// Create a request header for correlation
	requestHeader := RequestHeader{
		ApiKey:        0,
		ApiVersion:    0,
		CorrelationId: 12345,
		ClientId:      stringPtr("test-client"),
	}

	correlations := map[int32]RequestHeader{
		12345: requestHeader,
	}

	// Create and write a response
	original := Response{
		ResponseHeader: ResponseHeader{
			ApiKey:        requestHeader.ApiKey,
			ApiVersion:    requestHeader.ApiVersion,
			CorrelationId: 12345,
			ClientId:      requestHeader.ClientId,
		},
		Body: bytes.NewBuffer([]byte("response body data")),
	}

	var buf bytes.Buffer
	err := original.Write(&buf)
	if err != nil {
		t.Fatalf("Response.Write failed: %v", err)
	}

	readResponse, err := ReadResponse(&buf, correlations)
	if err != nil {
		t.Fatalf("ReadResponse failed: %v", err)
	}

	if readResponse.ApiKey != original.ApiKey {
		t.Errorf("ApiKey mismatch: expected %d, got %d", original.ApiKey, readResponse.ApiKey)
	}

	if readResponse.ApiVersion != original.ApiVersion {
		t.Errorf("ApiVersion mismatch: expected %d, got %d", original.ApiVersion, readResponse.ApiVersion)
	}

	if readResponse.CorrelationId != original.CorrelationId {
		t.Errorf("CorrelationId mismatch: expected %d, got %d", original.CorrelationId, readResponse.CorrelationId)
	}

	if original.ClientId != nil {
		if readResponse.ClientId == nil {
			t.Errorf("ClientId mismatch: expected %v, got nil", *original.ClientId)
		} else if *readResponse.ClientId != *original.ClientId {
			t.Errorf("ClientId mismatch: expected %v, got %v", *original.ClientId, *readResponse.ClientId)
		}
	}

	if !bytes.Equal(readResponse.Body.Bytes(), original.Body.Bytes()) {
		t.Errorf("Body mismatch: expected %v, got %v", original.Body.Bytes(), readResponse.Body.Bytes())
	}
}

func TestReadResponseMissingCorrelation(t *testing.T) {
	// Test reading response with missing correlation
	correlations := map[int32]RequestHeader{}

	response := Response{
		ResponseHeader: ResponseHeader{
			ApiKey:        0,
			ApiVersion:    0,
			CorrelationId: 99999,
		},
		Body: bytes.NewBuffer([]byte("body")),
	}

	var buf bytes.Buffer
	err := response.Write(&buf)
	if err != nil {
		t.Fatalf("Response.Write failed: %v", err)
	}

	_, err = ReadResponse(&buf, correlations)
	if err == nil {
		t.Error("ReadResponse should fail with missing correlation")
	}
}

func TestReadResponseEOF(t *testing.T) {
	// Test reading from empty reader
	emptyReader := bytes.NewReader([]byte{})
	correlations := map[int32]RequestHeader{}

	_, err := ReadResponse(emptyReader, correlations)
	if err != io.EOF {
		t.Errorf("Expected io.EOF for empty reader, got: %v", err)
	}
}

func TestRequestResponseRoundtrip(t *testing.T) {
	// Test a complete request/response roundtrip
	requestHeader := RequestHeader{
		ApiKey:        0,
		ApiVersion:    0,
		CorrelationId: 54321,
		ClientId:      stringPtr("roundtrip-client"),
	}

	request := Request{
		RequestHeader: requestHeader,
		Body:          bytes.NewBuffer([]byte("request body")),
	}

	// Write request
	var requestBuf bytes.Buffer
	err := request.Write(&requestBuf)
	if err != nil {
		t.Fatalf("Request.Write failed: %v", err)
	}

	// Read request back
	readRequest, err := ReadRequest(&requestBuf)
	if err != nil {
		t.Fatalf("ReadRequest failed: %v", err)
	}

	// Create response with same correlation
	correlations := map[int32]RequestHeader{
		readRequest.CorrelationId: readRequest.RequestHeader,
	}

	response := Response{
		ResponseHeader: ResponseHeader{
			ApiKey:        readRequest.ApiKey,
			ApiVersion:    readRequest.ApiVersion,
			CorrelationId: readRequest.CorrelationId,
			ClientId:      readRequest.ClientId,
		},
		Body: bytes.NewBuffer([]byte("response body")),
	}

	// Write response
	var responseBuf bytes.Buffer
	err = response.Write(&responseBuf)
	if err != nil {
		t.Fatalf("Response.Write failed: %v", err)
	}

	// Read response back
	readResponse, err := ReadResponse(&responseBuf, correlations)
	if err != nil {
		t.Fatalf("ReadResponse failed: %v", err)
	}

	// Verify correlation ID matches
	if readResponse.CorrelationId != readRequest.CorrelationId {
		t.Errorf("CorrelationId mismatch: request %d, response %d", readRequest.CorrelationId, readResponse.CorrelationId)
	}

	// Verify API key and version match
	if readResponse.ApiKey != readRequest.ApiKey {
		t.Errorf("ApiKey mismatch: request %d, response %d", readRequest.ApiKey, readResponse.ApiKey)
	}

	if readResponse.ApiVersion != readRequest.ApiVersion {
		t.Errorf("ApiVersion mismatch: request %d, response %d", readRequest.ApiVersion, readResponse.ApiVersion)
	}
}
