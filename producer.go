package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaProducer struct {
	host     string
	topic    string
	port     int
	producer *kafka.Producer
}

func NewKafkaProducer(host, topic string, port int) *KafkaProducer {
	kafkaProducer := &KafkaProducer{
		host:  host,
		port:  port,
		topic: topic,
	}

	if err := kafkaProducer.createProducer(); err != nil {
		panic(fmt.Sprintf("Could not create Kafka producer: %v", err))
	}

	return kafkaProducer
}

func (kp *KafkaProducer) createProducer() error {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": fmt.Sprintf("%s:%d", kp.host, kp.port)})
	if err != nil {
		return err
	}
	kp.producer = p

	return nil
}

func (kp *KafkaProducer) Produce(event []byte) {
	kp.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kp.topic, Partition: kafka.PartitionAny},
		Value:          event,
	}
}

func (kp *KafkaProducer) Teardown() {
	kp.producer.Close()
}
