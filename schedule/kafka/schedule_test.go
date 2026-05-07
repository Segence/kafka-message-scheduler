package kafka

import (
	"testing"

	confluent "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestSchedule_PreserveKey(t *testing.T) {
	type fields struct {
		Message *confluent.Message
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			name: "PreserveKey is not defined",
			fields: fields{
				Message: &confluent.Message{
					Headers: []confluent.Header{},
				},
			},
			want: false,
		},
		{
			name: "PreserveKey is defined and set to anything other than true",
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
			name: "PreserveKey is defined and set to true",
			fields: fields{
				Message: &confluent.Message{
					Headers: []confluent.Header{
						{
							Key:   PreserveKey,
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
			if got := s.PreserveKey(); got != tt.want {
				t.Errorf("PreserveKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
