package kafka_client

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
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
}

type KafkaMessage struct {
	Value     map[string]float64
	Timestamp time.Time
	Offset    kafka.Offset
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

	config := kafka.ConfigMap{
		"bootstrap.servers":  client.BootstrapServers,
		"group.id":           "kafka-datasource",
		"enable.auto.commit": "false",
	}

	if client.SecurityProtocol != "" {
		config.SetKey("security.protocol", client.SecurityProtocol)
	}
	if client.SaslMechanisms != "" {
		config.SetKey("sasl.mechanisms", client.SaslMechanisms)
	}
	if client.SaslUsername != "" {
		config.SetKey("sasl.username", client.SaslUsername)
	}
	if client.SaslPassword != "" {
		config.SetKey("sasl.password", client.SaslPassword)
	}
	if client.Debug != "" {
		config.SetKey("debug", client.Debug)
	}

	// TLS configuration
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		panic(err)
	}
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		panic(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
	}

	tlsConfig.InsecureSkipVerify = true // Change this based on security requirements
	tlsConfig.BuildNameToCertificate()

	config.SetKey("ssl.certificate.location", clientCertPath)
	config.SetKey("ssl.key.location", clientKeyPath)
	config.SetKey("ssl.ca.location", caCertPath)

	config.SetKey("security.protocol", "ssl")                  // Use SSL protocol
	config.SetKey("ssl.endpoint.identification.algorithm", "") // Allow insecure connections

	client.Consumer, err = kafka.NewConsumer(&config)
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
}

func (client *KafkaClient) TopicAssign(topic string, partition int32, autoOffsetReset string,
	timestampMode string) {
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
	partitions := []kafka.TopicPartition{topic_partition}
	err = client.Consumer.Assign(partitions)

	if err != nil {
		panic(err)
	}
}

func (client *KafkaClient) ConsumerPull() (KafkaMessage, kafka.Event) {
	var message KafkaMessage
	ev := client.Consumer.Poll(100)

	if ev == nil {
		return message, ev
	}

	switch e := ev.(type) {
	case *kafka.Message:
		json.Unmarshal([]byte(e.Value), &message.Value)
		message.Offset = e.TopicPartition.Offset
		message.Timestamp = e.Timestamp
	case kafka.Error:
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		if e.Code() == kafka.ErrAllBrokersDown {
			panic(e)
		}
	default:
	}
	return message, ev
}

func (client KafkaClient) HealthCheck() error {
	client.consumerInitialize("", "", "")

	_, err := client.Consumer.GetMetadata(nil, true, int(client.HealthcheckTimeout))

	if err != nil {
		if err.(kafka.Error).Code() == kafka.ErrTransport {
			return err
		}
	}

	return nil
}

func (client *KafkaClient) Dispose() {
	client.Consumer.Close()
}
