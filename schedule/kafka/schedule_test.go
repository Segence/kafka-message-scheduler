package kafka

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestSchedule_UseConfluentSchemaRegistry(t *testing.T) {
	type fields struct {
		Message *confluent.Message
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "UseConfluentSchemaRegistry is not defined",
			fields: fields{
				Message: &confluent.Message{
					Headers: []confluent.Header{},
				},
			},
			want: false,
		},
		{
			name: "UseConfluentSchemaRegistry is defined and set to anything other than true",
			fields: fields{
				Message: &confluent.Message{
					Headers: []confluent.Header{
						{
							Key:   "key",
							Value: []byte("value"),
						},
					},
				},
			},
			want: false,
		},
		{
			name: "UseConfluentSchemaRegistry is defined and set to true",
			fields: fields{
				Message: &confluent.Message{
					Headers: []confluent.Header{
						{
							Key:   UseConfluentSchemaRegistry,
							Value: []byte("true"),
						},
					},
				},
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Schedule{
				Message: tt.fields.Message,
			}
			if got := s.UseConfluentSchemaRegistry(); got != tt.want {
				t.Errorf("UseConfluentSchemaRegistry() = %v, want %v", got, tt.want)
			}
		})
	}
}
