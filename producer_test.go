package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	producer, _ := NewKafkaProducer("localhost:9092")
	assert.NotNil(t, producer)
	assert.Equal(t, "localhost:9092", producer.address)
}
