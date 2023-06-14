package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

type MessageData struct {
	Key       *string            `json:"key,omitempty" yaml:"key,omitempty"`
	Header    map[string]string `json:"header,omitempty" yaml:"header,omitempty"`
	Partition *int32             `json:"partition,omitempty" yaml:"partition,omitempty"`
	Topic     *string            `json:"topic,omitempty" yaml:"topic,omitempty"`
	Offset    *int64             `json:"offset,omitempty" yaml:"offset,omitempty"`
	Timestamp *time.Time         `json:"timestamp,omitempty" yaml:"timestamp,omitempty"`
	Message   string             `json:"message" yaml:"message"`
}

func consumeMessages(config map[string]interface{}, topics []string, showOptions []string, output string,
	outputFilePattern string, outputType string, offset int64, timestamp int64, group string) ([]MessageData, error) {

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               config["bootstrap_servers"],
		"security.protocol":               config["security_protocol"],
		"ssl.ca.location":                 config["ssl_cafile"],
		"ssl.certificate.location":        config["ssl_certfile"],
		"ssl.key.location":                config["ssl_keyfile"],
		"ssl.key.password":                config["ssl_key_password"],
		"ssl.key.is.password":             config["ssl_key_is_password"],
		"ssl.cipher.suites":               config["ssl_cipher_suites"],
		"ssl.curves.list":                 config["ssl_curves_list"],
		"ssl.keyform":                     config["ssl_keyform"],
		"ssl.keystore.location":           config["ssl_keystore_location"],
		"ssl.keystore.password":           config["ssl_keystore_password"],
		"ssl.keystore.type":               config["ssl_keystore_type"],
		"ssl.truststore.location":         config["ssl_truststore_location"],
		"ssl.truststore.password":         config["ssl_truststore_password"],
		"ssl.truststore.type":             config["ssl_truststore_type"],
		"ssl.keymanager.algorithm":        config["ssl_keymanager_algorithm"],
		"ssl.trustmanager.algorithm":      config["ssl_trustmanager_algorithm"],
		"ssl.endpoint.identification.algo": config["ssl_endpoint_identification_algo"],
		"group.id":                        group,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
	}

	partitions := make([]kafka.TopicPartition, len(topics))
	for i, topic := range topics {
		if strings.Contains(topic, ":") {
			topicPartition := strings.SplitN(topic, ":", 2)
			partitions[i] = kafka.TopicPartition{
				Topic:     &topicPartition[0],
				Partition: kafka.PartitionAny,
			}
		} else {
			partitions[i] = kafka.TopicPartition{
				Topic:     &topic,
				Partition: kafka.PartitionAny,
			}
		}
	}

	err = consumer.Assign(partitions)
	if err != nil {
		return nil, fmt.Errorf("failed to assign partitions: %w", err)
	}

	if offset != -1 {
		for _, partition := range partitions {
			consumer.Seek(partition, offset, 0)
		}
	} else if timestamp != -1 {
		for _, partition := range partitions {
			consumer.Seek(partition, timestamp, 0)
		}
	}

	messages := make([]MessageData, 0)
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			return nil, fmt.Errorf("failed to read message: %w", err)
		}

		data := MessageData{
			Message: string(msg.Value),
		}

		if contains(showOptions, "key") {
			key := msg.Key
			data.Key = &key
		}
		if contains(showOptions, "header") {
			headers := make(map[string]string)
			for _, header := range msg.Headers {
				headers[string(header.Key)] = string(header.Value)
			}
			data.Header = headers
		}
		if contains(showOptions, "partition") {
			partition := msg.TopicPartition.Partition
			data.Partition = &partition
		}
		if contains(showOptions, "topic") {
			topic := *msg.TopicPartition.Topic
			data.Topic = &topic
		}
		if contains(showOptions, "offset") {
			offset := msg.TopicPartition.Offset
			data.Offset = &offset
		}
		if contains(showOptions, "timestamp") {
			timestamp := msg.Timestamp
			data.Timestamp = &timestamp
		}

		messages = append(messages, data)

		if output != "" {
			fileName := strings.ReplaceAll(outputFilePattern, "{{topic}}", *msg.TopicPartition.Topic)
			fileName = strings.ReplaceAll(fileName, "{{partition}}", fmt.Sprint(msg.TopicPartition.Partition))
			fileName = strings.ReplaceAll(fileName, "{{offset}}", fmt.Sprint(msg.TopicPartition.Offset))
			fileName = strings.ReplaceAll(fileName, "{{extension}}", outputType)

			filePath := fmt.Sprintf("%s/%s", output, fileName)
			err := writeToFile(filePath, outputType, data)
			if err != nil {
				return nil, fmt.Errorf("failed to write message to file: %w", err)
			}
		}
	}
}

func contains(slice []string, value string) bool {
	for _, item := range slice {
		if item == value {
			return true
		}
	}
	return false
}

func writeToFile(filePath string, outputType string, data MessageData) error {
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	switch outputType {
	case "json":
		dataJSON, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to convert data to JSON: %w", err)
		}
		dataJSON = append(dataJSON, '\n')
		_, err = file.Write(dataJSON)
		if err != nil {
			return fmt.Errorf("failed to write JSON data to file: %w", err)
		}
	case "yaml", "yml":
		dataYAML, err := yaml.Marshal(data)
		if err != nil {
			return fmt.Errorf("failed to convert data to YAML: %w", err)
		}
		dataYAML = append(dataYAML, '\n')
		_, err = file.Write(dataYAML)
		if err != nil {
			return fmt.Errorf("failed to write YAML data to file: %w", err)
		}
	default:
		return fmt.Errorf("unsupported output type: %s", outputType)
	}

	return nil
}

func main() {
	configFile := flag.String("config", "", "Kafka connection and security configuration")
	topics := flag.String("topics", "", "Kafka topics to consume from")
	show := flag.String("show", "", "Comma-separated list of options to show (key, header, partition, topic, offset, timestamp)")
	output := flag.String("output", "", "Output folder for messages")
	outputFilePattern := flag.String("outputFilePattern", "{{topic}}-{{partition}}-{{offset}}.{{extension}}", "Output file name pattern using mustache syntax")
	outputType := flag.String("type", "json", "Output type")
	offset := flag.Int64("offset", -1, "Start consuming from the specified offset")
	timestamp := flag.Int64("timestamp", -1, "Start consuming from the specified timestamp")
	group := flag.String("group", "", "Consumer group name")
	flag.Parse()

	showOptions := strings.Split(*show, ",")
	configContent, err := ioutil.ReadFile(*configFile)
	if err != nil {
		fmt.Printf("failed to read Kafka configuration file: %s\n", err)
		os.Exit(1)
	}

	var config map[string]interface{}
	err = json.Unmarshal(configContent, &config)
	if err != nil {
		fmt.Printf("failed to parse Kafka configuration: %s\n", err)
		os.Exit(1)
	}

	messages, err := consumeMessages(config, strings.Split(*topics, ","), showOptions, *output, *outputFilePattern, *outputType, *offset, *timestamp, *group)
	if err != nil {
		fmt.Printf("failed to consume messages: %s\n", err)
		os.Exit(1)
	}

	if *outputType == "json" {
		messagesJSON, err := json.Marshal(messages)
		if err != nil {
			fmt.Printf("failed to convert messages to JSON: %s\n", err)
			os.Exit(1)
		}
		fmt.Println(string(messagesJSON))
	} else if *outputType == "yaml" || *outputType == "yml" {
		messagesYAML, err := yaml.Marshal(messages)
		if err != nil {
			fmt.Printf("failed to convert messages to YAML: %s\n", err)
			os.Exit(1)
		}
		fmt.Println(string(messagesYAML))
	} else {
		fmt.Printf("unsupported output type: %s\n", *outputType)
		os.Exit(1)
	}
}
