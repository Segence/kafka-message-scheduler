package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const (
	keySchemaIDHeader   = "__key_schema_id"
	valueSchemaIDHeader = "__value_schema_id"
	magicByteV0         = 0x0
	magicByteV1         = 0x1
)

type SchemaID struct {
	protocolVersion uint8
	schemaID        uint32
	schemaUUID      *uuid.UUID
}

// schemaIdFromBytes extracts the schema ID from a byte array
// TODO implement handling of message indexes for Protobuf: https://github.com/confluentinc/schema-registry/blob/3f6efe46468a58a14bda9777f792359ced157e61/schema-serializer/src/main/java/io/confluent/kafka/serializers/schema/id/SchemaId.java#L72
func schemaIdFromBytes(bytes []byte) (SchemaID, uint8, error) {
	version := bytes[0]

	if version == magicByteV0 {
		if len(bytes) < 7 {
			return SchemaID{}, 0, fmt.Errorf("invalid schema id length %d", len(bytes))
		}
		schemaID := binary.BigEndian.Uint32(bytes[1:5])
		return SchemaID{0, schemaID, nil}, 5, nil
	}

	if version == magicByteV1 {
		if len(bytes) < 19 {
			return SchemaID{}, 0, fmt.Errorf("invalid schema id length %d", len(bytes))
		}
		guid, err := uuid.FromBytes(bytes[1:17])
		return SchemaID{1, 0, &guid}, 17, err
	}

	return SchemaID{}, 0, errors.New("unknown magic byte")
}

// schemaIdToBytes converts a SchemaID into byte array
// TODO implement handling of message indexes for Protobuf: https://github.com/confluentinc/schema-registry/blob/3f6efe46468a58a14bda9777f792359ced157e61/schema-serializer/src/main/java/io/confluent/kafka/serializers/schema/id/SchemaId.java#L85
func schemaIdToBytes(schemaID SchemaID) ([]byte, error) {
	if schemaID.protocolVersion == 0 {
		binarySchemaID := make([]byte, 1)
		binarySchemaID[0] = magicByteV0
		return binary.BigEndian.AppendUint32(binarySchemaID, schemaID.schemaID), nil
	}

	if schemaID.protocolVersion == 1 {
		schemaIDBytes, err := schemaID.schemaUUID.MarshalBinary()

		if err != nil {
			return nil, err
		}

		return append([]byte{magicByteV1}, schemaIDBytes...), nil
	}

	return nil, errors.New("unknown schema protocol version")
}

func SerializeGenericSchema(schemaID SchemaID, payload []byte, schemaIDInHeader bool, isKey bool) ([]byte, *kafka.Header, error) {

	schemaIDBytes, err := schemaIdToBytes(schemaID)

	if err != nil {
		return nil, nil, err
	}

	if schemaIDInHeader {

		if schemaID.protocolVersion != 1 {
			return nil, nil, fmt.Errorf("invalid protocol version, must be 1, got %d", schemaID.protocolVersion)
		}

		if isKey {
			return payload, &kafka.Header{keySchemaIDHeader, schemaIDBytes}, nil
		}
		return payload, &kafka.Header{valueSchemaIDHeader, schemaIDBytes}, nil
	}

	return append(schemaIDBytes, payload...), nil, nil
}

func DeserializeGenericSchema(payload []byte, isKey bool, headers []kafka.Header) (SchemaID, []byte, bool, error) {

	headerKey := keySchemaIDHeader
	if !isKey {
		headerKey = valueSchemaIDHeader
	}

	var headerValue []byte

	for _, h := range headers {
		if h.Key == headerKey {
			headerValue = h.Value
			break
		}
	}

	if headerValue != nil {
		schemaID, _, err := schemaIdFromBytes(headerValue)
		if err != nil {
			return SchemaID{}, nil, false, err
		}
		return schemaID, payload, true, nil
	}

	schemaID, payloadOffset, err := schemaIdFromBytes(payload)
	if err != nil {
		return SchemaID{}, nil, false, err
	}
	return schemaID, payload[payloadOffset:], false, nil
}
