package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
)

type File struct {
	// NOTE:  Configuration for producer and consumer are kept separate so that librdkafka does not
	// log warnings due to unrecognized options for a producer or consumer.  However, some settings
	// might be common to both the consumer and the producer, and those are stored in the common
	// configuration.

	KafkaCommonConfiguration    confluent.ConfigMap `yaml:"kafka.common.configuration,omitempty"`
	KafkaProducerConfiguration  confluent.ConfigMap `yaml:"kafka.producer.configuration,omitempty"`
	KafkaConsumerConfiguration  confluent.ConfigMap `yaml:"kafka.consumer.configuration,omitempty"`
	SchemaRegistryConfiguration confluent.ConfigMap `yaml:"kafka.schema-registry.configuration,omitempty"`
}

func (f *File) GenerateProducerConfiguration() confluent.ConfigMap {
	result := make(confluent.ConfigMap, len(f.KafkaCommonConfiguration)+len(f.KafkaProducerConfiguration))
	for k, v := range f.KafkaCommonConfiguration {
		result[k] = v
	}
	for k, v := range f.KafkaProducerConfiguration {
		result[k] = v
	}
	return result
}

func (f *File) GenerateConsumerConfiguration() confluent.ConfigMap {
	result := make(confluent.ConfigMap, len(f.KafkaCommonConfiguration)+len(f.KafkaConsumerConfiguration))
	for k, v := range f.KafkaCommonConfiguration {
		result[k] = v
	}
	for k, v := range f.KafkaConsumerConfiguration {
		result[k] = v
	}
	return result
}

func (f *File) GenerateSchemaRegistryConfiguration() (*schemaregistry.Config, error) {

	if len(f.SchemaRegistryConfiguration) < 1 {
		return nil, nil
	}

	result := make(confluent.ConfigMap, len(f.SchemaRegistryConfiguration))
	for k, v := range f.SchemaRegistryConfiguration {
		result[k] = v
	}

	if result["url"] == nil {
		return nil, fmt.Errorf("schema registry configuration must contain url")
	}

	if result["authentication_type"] != nil {

		switch result["authentication_type"] {
		case "basic":
			if result["basic_auth_user_info"] == nil {
				return nil, fmt.Errorf("schema registry configuration with basic authentication must contain basic_auth_user_info")
			}

			return schemaregistry.NewConfigWithAuthentication(
				result["url"].(string),
				result["username"].(string),
				result["password"].(string),
			), nil
		default:
			return nil, fmt.Errorf("unsupported authentication type: %s", result["authentication_type"])
		}

	}

	return schemaregistry.NewConfig(result["url"].(string)), nil
}

func ReadFile(filePath string) (File, error) {
	body, err := os.ReadFile(filePath)
	if err != nil {
		return File{}, fmt.Errorf("reading configuration file: %w", err)
	}
	var result File
	err = yaml.UnmarshalStrict(body, &result)
	if err != nil {
		return File{}, fmt.Errorf("parsing configuration file: %w", err)
	}
	return result, nil
}

func getString(name, defaultValue string) string {
	value, set := os.LookupEnv(name)
	if set {
		return value
	}
	return defaultValue
}

func getInt(name string, defaultValue int) int {
	value, set := os.LookupEnv(name)
	if !set {
		return defaultValue
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}
	return i
}

func LogLevel() log.Level {
	lvl, err := log.ParseLevel(getString("LOG_LEVEL", "info"))
	if err != nil {
		return log.InfoLevel
	}
	return lvl
}

func ConfigurationFile() string {
	return getString("CONFIGURATION_FILE", "")
}

func GraylogServer() string {
	return getString("GRAYLOG_SERVER", "")
}

func MetricsAddr() string {
	return getString("METRICS_ADDR", ":8001")
}

func ServerAddr() string {
	return getString("SERVER_ADDR", ":8000")
}

func BootstrapServers() string {
	return getString("BOOTSTRAP_SERVERS", "localhost:9092")
}

func GroupID() string {
	return getString("GROUP_ID", "scheduler-cg")
}

func SessionTimeout() int {
	return getInt("SESSION_TIMEOUT", 6000)
}

func SinceDelta() int {
	return getInt("SINCE_DELTA", 0)
}

func SchedulesTopics() []string {
	return strings.Split(getString("SCHEDULES_TOPICS", "schedules"), ",")
}

func HistoryTopic() string {
	return getString("HISTORY_TOPIC", "history")
}

func ScheduleGraceInterval() int {
	return getInt("SCHEDULE_GRACE_INTERVAL", 0)
}
