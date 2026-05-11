package kafka

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/uuid"
)

const otherHeader = "other_header"

var (
	schemaGUIDBytes = []byte{
		0x70, 0x96, 0x99, 0x33, 0x37, 0x87, 0x73, 0x59, 0x15, 0x15, 0x12, 0x19, 0x90, 0x13, 0x23, 0x14,
	}
)

func Test_schemaIdFromBytes(t *testing.T) {

	expectedSchemaUUID, err := uuid.FromBytes(schemaGUIDBytes)

	if err != nil {
		panic(err)
	}

	type args struct {
		bytes []byte
	}
	tests := []struct {
		name              string
		args              args
		wantSchemaID      SchemaID
		wantPayloadOffset uint8
		wantErr           bool
	}{
		{
			name: "invalid magic byte",
			args: args{
				bytes: []byte{0x2},
			},
			wantErr: true,
		},
		{
			name: "using V0 format and missing schema ID and payload",
			args: args{
				bytes: []byte{0x0},
			},
			wantErr: true,
		},
		{
			name: "using V0 format and missing payload",
			args: args{
				bytes: []byte{0x0, 0x0, 0x0, 0x0, 0x2},
			},
			wantErr: true,
		},
		{
			name: "using V0 format and has valid payload",
			args: args{
				bytes: []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2},
			},
			wantSchemaID: SchemaID{
				protocolVersion: 0,
				schemaID:        2,
				schemaUUID:      nil,
			},
			wantPayloadOffset: 5,
			wantErr:           false,
		},
		{
			name: "using V1 format and missing schema ID and payload",
			args: args{
				bytes: []byte{0x1},
			},
			wantErr: true,
		},
		{
			name: "using V1 format and missing payload",
			args: args{
				bytes: []byte{0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2},
			},
			wantErr: true,
		},
		{
			name: "using V1 format and has valid payload",
			args: args{
				bytes: append(append([]byte{0x1}, schemaGUIDBytes...), []byte{0x1, 0x2}...),
			},
			wantSchemaID: SchemaID{
				protocolVersion: 1,
				schemaID:        0,
				schemaUUID:      &expectedSchemaUUID,
			},
			wantPayloadOffset: 17,
			wantErr:           false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaID, payloadOffset, err := schemaIdFromBytes(tt.args.bytes)
			if (err != nil) != tt.wantErr {
				t.Errorf("schemaIdFromBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(schemaID, tt.wantSchemaID) {
				t.Errorf("schemaIdFromBytes() schemaID = %v, want %v", schemaID, tt.wantSchemaID)
			}
			if payloadOffset != tt.wantPayloadOffset {
				t.Errorf("schemaIdFromBytes() payloadOffset = %v, want %v", payloadOffset, tt.wantPayloadOffset)
			}
		})
	}
}

func Test_schemaIdToBytes(t *testing.T) {

	schemaUUID, err := uuid.FromBytes(schemaGUIDBytes)

	if err != nil {
		panic(err)
	}

	type args struct {
		schemaID SchemaID
	}
	tests := []struct {
		name    string
		args    args
		want    []byte
		wantErr bool
	}{
		{
			name: "invalid schema protocol version",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 2,
					schemaID:        0,
					schemaUUID:      nil,
				},
			},
			wantErr: true,
		},
		{
			name: "valid V0 schema ID",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 0,
					schemaID:        2,
					schemaUUID:      nil,
				},
			},
			want:    []byte{0x0, 0x0, 0x0, 0x0, 0x2},
			wantErr: false,
		},
		{
			name: "valid V1 schema ID",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 1,
					schemaID:        0,
					schemaUUID:      &schemaUUID,
				},
			},
			want:    append([]byte{0x1}, schemaGUIDBytes...),
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaIDBytes, err := schemaIdToBytes(tt.args.schemaID)
			if (err != nil) != tt.wantErr {
				t.Errorf("schemaIdToBytes() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(schemaIDBytes, tt.want) {
				t.Errorf("schemaIdToBytes() got = %v, want %v", schemaIDBytes, tt.want)
			}
		})
	}
}

func TestSerializeGenericSchema(t *testing.T) {

	schemaUUID, err := uuid.FromBytes(schemaGUIDBytes)

	if err != nil {
		panic(err)
	}

	type args struct {
		schemaID         SchemaID
		payload          []byte
		schemaIDInHeader bool
		isKey            bool
	}
	tests := []struct {
		name            string
		args            args
		want            []byte
		wantMaybeHeader *kafka.Header
		wantErr         bool
	}{
		{
			name: "failed to serialize schema",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 2,
				},
				payload:          []byte{0x0, 0x0, 0x0, 0x0, 0x0},
				schemaIDInHeader: false,
				isKey:            false,
			},
			wantErr: true,
		},
		{
			name: "successfully serialize V0 schema in payload",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 0,
					schemaID:        2,
					schemaUUID:      nil,
				},
				payload:          []byte{0x1, 0x2},
				schemaIDInHeader: false,
				isKey:            false,
			},
			want:            []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2},
			wantMaybeHeader: nil,
			wantErr:         false,
		},
		{
			name: "fail when serializing V0 key schema in header",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 0,
					schemaID:        2,
					schemaUUID:      nil,
				},
				payload:          []byte{0x1, 0x2},
				schemaIDInHeader: true,
				isKey:            true,
			},
			wantErr: true,
		},
		{
			name: "successfully serialize V1 key schema in header",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 1,
					schemaID:        0,
					schemaUUID:      &schemaUUID,
				},
				payload:          []byte{0x1, 0x2},
				schemaIDInHeader: true,
				isKey:            true,
			},
			want:            []byte{0x1, 0x2},
			wantMaybeHeader: &kafka.Header{keySchemaIDHeader, append([]byte{0x1}, schemaGUIDBytes...)},
			wantErr:         false,
		},
		{
			name: "successfully serialize V1 value schema in header",
			args: args{
				schemaID: SchemaID{
					protocolVersion: 1,
					schemaID:        0,
					schemaUUID:      &schemaUUID,
				},
				payload:          []byte{0x1, 0x2},
				schemaIDInHeader: true,
				isKey:            false,
			},
			want:            []byte{0x1, 0x2},
			wantMaybeHeader: &kafka.Header{valueSchemaIDHeader, append([]byte{0x1}, schemaGUIDBytes...)},
			wantErr:         false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, maybeHeader, err := SerializeGenericSchema(tt.args.schemaID, tt.args.payload, tt.args.schemaIDInHeader, tt.args.isKey)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerializeGenericSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SerializeGenericSchema() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(maybeHeader, tt.wantMaybeHeader) {
				t.Errorf("SerializeGenericSchema() maybeHeader = %v, want %v", maybeHeader, tt.wantMaybeHeader)
			}
		})
	}
}

func TestDeserializeGenericSchema(t *testing.T) {
	type args struct {
		payload []byte
		isKey   bool
		headers []kafka.Header
	}
	tests := []struct {
		name                   string
		args                   args
		wantSchemaID           SchemaID
		wantPayload            []byte
		deserializedFromHeader bool
		wantErr                bool
	}{
		{
			name: "failed to deserialize schema from payload",
			args: args{
				payload: []byte{0x2, 0x0, 0x0, 0x0, 0x0},
				isKey:   true,
				headers: []kafka.Header{
					{otherHeader, append([]byte{0x1}, schemaGUIDBytes...)},
				},
			},
			wantErr: true,
		},
		{
			name: "failed to deserialize schema",
			args: args{
				payload: []byte{0x0, 0x0, 0x0, 0x0, 0x0},
				isKey:   true,
				headers: []kafka.Header{
					{keySchemaIDHeader, []byte{0x0}},
				},
			},
			wantErr: true,
		},
		{
			name: "successfully deserialize schema from payload",
			args: args{
				payload: []byte{0x0, 0x0, 0x0, 0x0, 0x2, 0x1, 0x2},
				isKey:   true,
				headers: []kafka.Header{},
			},
			wantSchemaID: SchemaID{
				protocolVersion: 0,
				schemaID:        2,
				schemaUUID:      nil,
			},
			wantPayload: []byte{0x1, 0x2},
			wantErr:     false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			schemaID, payload, deserializedFromHeader, err := DeserializeGenericSchema(tt.args.payload, tt.args.isKey, tt.args.headers)
			if (err != nil) != tt.wantErr {
				t.Errorf("DeserializeGenericSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(schemaID, tt.wantSchemaID) {
				t.Errorf("DeserializeGenericSchema() got = %v, want %v", schemaID, tt.wantSchemaID)
			}
			if !reflect.DeepEqual(payload, tt.wantPayload) {
				t.Errorf("DeserializeGenericSchema() got1 = %v, want %v", payload, tt.wantPayload)
			}
			if deserializedFromHeader != tt.deserializedFromHeader {
				t.Errorf("DeserializeGenericSchema() deserializedFromHeader = %v, want %v", deserializedFromHeader, tt.deserializedFromHeader)
			}
		})
	}
}
