package kafka

import (
	"os"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	address  string
	producer *kafka.Producer
}

func NewKafkaProducer(address string) *KafkaProducer {
	kafkaProducer := &KafkaProducer{
		address:  address,
	}

	if err := kafkaProducer.createProducer(); err != nil {
		fmt.Printf("Could not create Kafka producer: %v", err)
		os.Exit(1)
	}

	return kafkaProducer
}

func (kp *KafkaProducer) createProducer() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kp.address})
	if err != nil {
		return err
	}
	kp.producer = p

	return nil
}

func (kp *KafkaProducer) Produce(event []byte, topic string) {
	kp.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          event,
	}
}

func (kp *KafkaProducer) Teardown() {
	kp.producer.Close()
}
