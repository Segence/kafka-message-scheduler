package test

import (
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

func SerializeAvro(message interface{}, schemaRegistryClient schemaregistry.Client, topicName string, serdeType serde.Type) ([]byte, error) {

	ser, err := avro.NewGenericSerializer(schemaRegistryClient, serdeType, avro.NewSerializerConfig())

	if err != nil {
		return nil, err
	}

	return ser.Serialize(topicName, message)
}

func DeserializeAvro[T interface{}](message []byte, schemaRegistryClient schemaregistry.Client, topicName string, serdeType serde.Type, result *T) error {

	ser, err := avro.NewGenericDeserializer(schemaRegistryClient, serdeType, avro.NewDeserializerConfig())

	if err != nil {
		return err
	}

	return ser.DeserializeInto(topicName, message, result)
}
