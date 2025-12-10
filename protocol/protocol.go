package protocol

import "fmt"

////////////////////
// Methods to decode and encode requests and responses
////////////////////

func DecodeRequest(bytes []byte) (Request, error) {
	request := Request{}
	offset := 0

	size, c, err := DecodeInt32(bytes[offset:])
	//c, err := binary.Decode(bytes, binary.BigEndian, &request.Size)
	if err != nil {
		fmt.Println("Failed to decode request size", err)
		return request, err
	}
	request.Size = size
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	apiKey, c, err := DecodeInt16(bytes[offset:])
	//c, err = binary.Decode(bytes[offset:], binary.BigEndian, &request.ApiKey)
	if err != nil {
		fmt.Println("Failed to decode apiKey", err)
		return request, err
	}
	request.ApiKey = apiKey
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	apiVersion, c, err := DecodeInt16(bytes[offset:])
	//c, err = binary.Decode(bytes[offset:], binary.BigEndian, &request.ApiVersion)
	if err != nil {
		fmt.Println("Failed to decode apiVersion", err)
		return request, err
	}
	request.ApiVersion = apiVersion
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	correlationId, c, err := DecodeInt32(bytes[offset:])
	//c, err = binary.Decode(bytes[offset:], binary.BigEndian, &request.CorrelationId)
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return request, err
	}
	request.CorrelationId = correlationId
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	// Decode client ID
	clientId, c, err := DecodeNullableString(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode clientId", err)
		return request, err
	}
	offset += c
	request.ClientId = clientId
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	// Decode tagged fields
	_, c, err = DecodeUvarint(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode tagged fields", err)
		return request, err
	}
	offset += c
	fmt.Printf("Tagged fields length: %d\n", c)
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	request.Body = bytes[offset : request.Size+4] // We add 4 here because the first 4 bytes are the size

	return request, nil
}

func DecodeResponse(bytes []byte, correlations map[int32]RequestHeader) (Response, error) {
	response := Response{}
	offset := 0

	size, c, err := DecodeInt32(bytes[offset:])
	//c, err := binary.Decode(bytes, binary.BigEndian, &request.Size)
	if err != nil {
		fmt.Println("Failed to decode request size", err)
		return response, err
	}
	response.Size = size
	offset += c
	fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)

	correlationId, c, err := DecodeInt32(bytes[offset:])
	if err != nil {
		fmt.Println("Failed to decode correlationID", err)
		return response, err
	}
	offset += c

	requestHeader, ok := correlations[correlationId]
	if !ok {
		return response, fmt.Errorf("no correlation found for correlationId %d", correlationId)
	}

	response.ApiKey = requestHeader.ApiKey
	response.ApiVersion = requestHeader.ApiVersion
	response.CorrelationId = correlationId
	response.ClientId = requestHeader.ClientId

	if response.ApiKey != 18 {
		// Decode tagged fields
		l, c, err := DecodeUvarint(bytes[offset:])
		if err != nil {
			fmt.Println("Failed to decode tagged fields", err)
			return response, err
		}
		offset += c
		fmt.Printf("Tagged fields length: %d bytes, %d records\n", c, l)
		fmt.Printf("Ofsers ... c: %d; offset: %d\n", c, offset)
	}

	response.Body = bytes[offset : response.Size+4] // We add 4 here because the first 4 bytes are the size

	return response, nil
}
