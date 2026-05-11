package helper

import (
	"fmt"
	"os"
)

// tells if the tests is running in docker
func IsRunningInDocker() bool {
	if _, err := os.Stat("/.dockerenv"); os.IsNotExist(err) {
		return false
	}

	return true
}

// Get the bootstrap servers because in or out the docker the kafka server is different
func GetDefaultBootstrapServers() string {
	if IsRunningInDocker() {
		fmt.Println("kafka bootstrap servers=broker:29092")
		return "broker:29092"
	}
	fmt.Println("kafka bootstrap servers=localhost:9092")
	return "localhost:9092"
}
