package config

import (
	"strings"
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type schemaRegistryTestCase struct {
	name                      string
	file                      File
	expectError               bool
	expectedErrorContains     string
	expectedURL               string
	expectedBasicAuthUserInfo string
}

func TestNoSchemaRegistryConfigurationProvided(t *testing.T) {
	file := File{SchemaRegistryConfiguration: confluent.ConfigMap{}}
	result, err := file.GenerateSchemaRegistryConfiguration()

	if result != nil {
		t.Fatalf("Expected nil but got %v", result)
	}

	if err != nil {
		t.Fatalf("Error generating schema registry configuration: %v", err)
	}
}

func TestGenerateSchemaRegistryConfiguration(t *testing.T) {
	for _, tc := range []schemaRegistryTestCase{
		{
			name: "missing url",
			file: File{SchemaRegistryConfiguration: confluent.ConfigMap{
				"authentication_type": "oauth",
			}},
			expectError:           true,
			expectedErrorContains: "schema registry configuration must contain url",
		},
		{
			name: "unsupported authentication type",
			file: File{SchemaRegistryConfiguration: confluent.ConfigMap{
				"url":                 "http://registry.local",
				"authentication_type": "oauth",
			}},
			expectError:           true,
			expectedErrorContains: "unsupported authentication type: oauth",
		},
		{
			name: "basic authentication missing basic_auth_user_info",
			file: File{SchemaRegistryConfiguration: confluent.ConfigMap{
				"url":                 "http://registry.local",
				"authentication_type": "basic",
				"username":            "user",
				"password":            "secret",
			}},
			expectError:           true,
			expectedErrorContains: "schema registry configuration with basic authentication must contain basic_auth_user_info",
		},
		{
			name: "valid basic authentication",
			file: File{SchemaRegistryConfiguration: confluent.ConfigMap{
				"url":                  "http://registry.local",
				"authentication_type":  "basic",
				"basic_auth_user_info": "user:secret",
				"username":             "user",
				"password":             "secret",
			}},
			expectedURL:               "http://registry.local",
			expectedBasicAuthUserInfo: "user:secret",
		},
		{
			name: "valid url only configuration",
			file: File{SchemaRegistryConfiguration: confluent.ConfigMap{
				"url": "http://registry.local",
			}},
			expectedURL: "http://registry.local",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			config, err := tc.file.GenerateSchemaRegistryConfiguration()
			if tc.expectError {
				if err == nil {
					t.Fatalf("expected error but got nil")
				}
				if tc.expectedErrorContains != "" && !strings.Contains(err.Error(), tc.expectedErrorContains) {
					t.Fatalf("expected error to contain %q, got %q", tc.expectedErrorContains, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if config == nil {
				t.Fatal("expected non-nil schema registry config")
			}
			if config.SchemaRegistryURL != tc.expectedURL {
				t.Fatalf("expected SchemaRegistryURL %q, got %q", tc.expectedURL, config.SchemaRegistryURL)
			}
			if config.BasicAuthUserInfo != tc.expectedBasicAuthUserInfo {
				t.Fatalf("expected BasicAuthUserInfo %q, got %q", tc.expectedBasicAuthUserInfo, config.BasicAuthUserInfo)
			}
			if tc.expectedBasicAuthUserInfo != "" && config.BasicAuthCredentialsSource != "USER_INFO" {
				t.Fatalf("expected BasicAuthCredentialsSource USER_INFO, got %q", config.BasicAuthCredentialsSource)
			}
		})
	}
}
