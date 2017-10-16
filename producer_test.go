package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewProducer(t *testing.T) {
	producer := NewKafkaProducer("localhost", "allan", 9092)
	assert.NotNil(t, producer)
	assert.Equal(t, "localhost", producer.host)
	assert.Equal(t, "allan", producer.topic)
	assert.Equal(t, 9092, producer.port)
}
