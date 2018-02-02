package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Producer holds the broker address and the Confluent Kafka producer
type Producer struct {
	address  string
	producer *kafka.Producer
}

// NewProducer returns a new producer with bootstrap.servers set to the supplied address
func NewProducer(address string) (*Producer, error) {
	kafkaProducer := &Producer{
		address:  address,
	}

	if err := kafkaProducer.createProducer(); err != nil {
		return nil, fmt.Errorf("Could not create Kafka producer: %v", err)
	}

	return kafkaProducer, nil
}

func (kp *Producer) createProducer() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kp.address})
	if err != nil {
		return err
	}
	kp.producer = p

	return nil
}

// Produce sends the event to the given topic
func (kp *Producer) Produce(event []byte, topic string) {
	kp.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          event,
	}
}

// GetMetadata returns the metadata of all available topics in the Kafka cluster
func (kp *Producer) GetMetadata(timeout int) (*kafka.Metadata, error) {
	metadata, err := kp.producer.GetMetadata(nil, true, timeout)
	if err != nil {
		return nil, err
	}
	return metadata, nil
}

// Teardown closes the producer instance
func (kp *Producer) Teardown() {
	kp.producer.Close()
}
