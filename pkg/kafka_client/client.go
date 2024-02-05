package kafka_client

import (
	"encoding/json"
	"fmt"
	"github.com/grafana/grafana-plugin-sdk-go/backend/log"
	"github.com/hamba/avro"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const MAX_EARLIEST int64 = 100

type Options struct {
	BootstrapServers string `json:"bootstrapServers"`
	SecurityProtocol string `json:"securityProtocol"`
	SaslMechanisms   string `json:"saslMechanisms"`
	SaslUsername     string `json:"saslUsername"`
	SaslPassword     string `json:"saslPassword"`
	// TODO: If Debug is before HealthcheckTimeout, then json.Unmarshall
	// silently fails to parse the timeout from the s.JSONData.  Figure out why.
	HealthcheckTimeout int32  `json:"healthcheckTimeout"`
	Debug              string `json:"debug"`
	DataType           string `json:"dataType"`
}

type KafkaClient struct {
	Consumer           *kafka.Consumer
	BootstrapServers   string
	TimestampMode      string
	SecurityProtocol   string
	SaslMechanisms     string
	SaslUsername       string
	SaslPassword       string
	Debug              string
	HealthcheckTimeout int32
	DataType           string
}

type Data struct {
	Name           string  `json:"name" avro:"name"`
	ValueTimestamp string  `json:"valuetimestamp" avro:"valuetimestamp"`
	Quality        string  `json:"quality" avro:"quality"`
	Value          float64 `json:"value" avro:"value"`
}

type KafkaMessage struct {
	Value     Data
	Timestamp time.Time
	Offset    kafka.Offset
	Topic     string
}

func NewKafkaClient(options Options) KafkaClient {
	client := KafkaClient{
		BootstrapServers:   options.BootstrapServers,
		SecurityProtocol:   options.SecurityProtocol,
		SaslMechanisms:     options.SaslMechanisms,
		SaslUsername:       options.SaslUsername,
		SaslPassword:       options.SaslPassword,
		Debug:              options.Debug,
		HealthcheckTimeout: options.HealthcheckTimeout,
		DataType:           options.DataType,
	}
	return client
}

func (client *KafkaClient) consumerInitialize(caCertPath, clientCertPath, clientKeyPath string) {
	var err error
	/*
		caCertPath would be the path to ca-cert.pem.
		clientCertPath would be the path to either kafka-cert.pem or grafana-cert.pem,
				depending on which one you're using.
		clientKeyPath would be the path to the corresponding private key, either kafka-key.pem or grafana-key.pem.*/
	log.DefaultLogger.Info("consumerInitialize called")
	config := kafka.ConfigMap{
		"bootstrap.servers":  client.BootstrapServers,
		"group.id":           "kafka-datasource",
		"enable.auto.commit": "false",
	}
	//if client.SecurityProtocol != "" {
	//	log.DefaultLogger.Info("setting SecurityProtocol", "SecurityProtocol", client.SecurityProtocol)
	//	config.SetKey("security.protocol", client.SecurityProtocol)
	//}
	//if client.SaslMechanisms != "" {
	//	log.DefaultLogger.Info("setting SaslMechanisms", "SaslMechanisms", client.SaslMechanisms)
	//	config.SetKey("sasl.mechanisms", client.SaslMechanisms)
	//	log.DefaultLogger.Info("setting SaslUsername", "SaslUsername", client.SaslUsername)
	//	config.SetKey("sasl.username", client.SaslUsername)
	//	log.DefaultLogger.Info("setting SaslUsername", "SaslUsername", client.SaslUsername)
	//	config.SetKey("sasl.password", client.SaslUsername)
	//}
	//if client.SaslMechanisms != "" {
	//	config.SetKey("sasl.username", client.SaslUsername)
	//}
	//if client.SaslMechanisms != "" {
	//	config.SetKey("sasl.password", client.SaslPassword)
	//}
	//if client.Debug != "" {
	//	config.SetKey("debug", client.Debug)
	//}

	client.Consumer, err = kafka.NewConsumer(&config)
	if err != nil {
		log.DefaultLogger.Error(fmt.Sprintf("error during NewConsumer: %v", err))
	}
}

func (client *KafkaClient) TopicAssign(topic string, partition int32, autoOffsetReset string,
	timestampMode string) {
	log.DefaultLogger.Info("topicAssign called", "topic", topic)
	client.consumerInitialize("", "", "")
	client.TimestampMode = timestampMode
	var err error
	var offset int64
	var high, low int64
	switch autoOffsetReset {
	case "latest":
		offset = int64(kafka.OffsetEnd)
	case "earliest":
		low, high, err = client.Consumer.QueryWatermarkOffsets(topic, partition, 100)
		if err != nil {
			panic(err)
		}
		if high-low > MAX_EARLIEST {
			offset = high - MAX_EARLIEST
		} else {
			offset = low
		}
	default:
		offset = int64(kafka.OffsetEnd)
	}

	topic_partition := kafka.TopicPartition{
		Topic:     &topic,
		Partition: partition,
		Offset:    kafka.Offset(offset),
		Metadata:  new(string),
		Error:     err,
	}
	//if len(client.Partitions) == 0 {
	//	client.Partitions = make([]kafka.TopicPartition, 0)
	//}
	//client.Partitions = append(client.Partitions, topic_partition)
	////partitions := []kafka.TopicPartition{topic_partition}
	//err = client.Consumer.Assign(client.Partitions)
	partitions := []kafka.TopicPartition{topic_partition}
	err = client.Consumer.Assign(partitions)

	if err != nil {
		log.DefaultLogger.Error(fmt.Sprintf("error during subscribing to topic: %s err: %v", topic, err))
	}
}

func (client *KafkaClient) ConsumerPull() (KafkaMessage, kafka.Event) {
	var message KafkaMessage
	ev := client.Consumer.Poll(100)

	if ev == nil {
		return KafkaMessage{}, ev
	}

	switch e := ev.(type) {
	case *kafka.Message:
		if client.DataType == "AVRO" {
			schema, err := avro.Parse(`{ "type":"record", "name":"Data", "fields": [{"name":"name","type":"string"},{"name":"quality","type":"string"},{"name":"valuetimestamp","type":"string"}, {"name":"value","type":"double"}]}`)
			err = avro.Unmarshal(schema, e.Value, &message.Value)
			if err != nil {
				log.DefaultLogger.Error(fmt.Sprintf("error while unmarshall called: %v", err.Error()))
				return KafkaMessage{}, nil
			}
			log.DefaultLogger.Info(fmt.Sprintf("unmarshall done msg: %v", message.Value))
		} else {
			err := json.Unmarshal([]byte(e.Value), &message.Value)
			if err != nil {
				log.DefaultLogger.Error(fmt.Sprintf("unmarshall error: %v", e))
				return KafkaMessage{}, nil
			}
		}
		message.Offset = e.TopicPartition.Offset
		message.Timestamp = e.Timestamp
		message.Topic = *e.TopicPartition.Topic
	case kafka.Error:
		log.DefaultLogger.Error(fmt.Sprintf("in error block: %v", e))
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		if e.Code() == kafka.ErrAllBrokersDown {
			log.DefaultLogger.Error(fmt.Sprintf("in error block: %v", e))
			//panic(e)
			return KafkaMessage{}, nil
		}
	default:
		log.DefaultLogger.Info("in default block")
	}
	return message, ev
}

func (client KafkaClient) HealthCheck() error {
	log.DefaultLogger.Info("healthcheck called")
	client.consumerInitialize("", "", "")

	topic := ""
	_, err := client.Consumer.GetMetadata(&topic, false, 200)

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTransport {
			return err
		}
	}

	return nil
}

func (client *KafkaClient) Dispose() {
	if client.Consumer != nil {
		client.Consumer.Close()
	}
}
