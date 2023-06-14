package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/yaml.v2"
)

// Configuration struct
type Configuration struct {
	BootstrapServers string `json:"bootstrap_servers" yaml:"bootstrap_servers"`
	SecurityProtocol string `json:"security_protocol" yaml:"security_protocol"`
	SSLCAFile        string `json:"ssl_cafile" yaml:"ssl_cafile"`
	SSLCertFile      string `json:"ssl_certfile" yaml:"ssl_certfile"`
	SSLKeyFile       string `json:"ssl_keyfile" yaml:"ssl_keyfile"`
	Key              string `json:"key" yaml:"key"`
	Header           string `json:"header" yaml:"header"`
	Type             string `json:"type" yaml:"type"`
	Multi            bool   `json:"multi" yaml:"multi"`
}

// Message struct
type Message map[string]interface{}

// Load configuration from JSON or YAML file
func loadConfig(configPath string) (*Configuration, error) {
	content, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	var config Configuration
	if strings.HasSuffix(configPath, ".json") {
		err = json.Unmarshal(content, &config)
	} else if strings.HasSuffix(configPath, ".yaml") || strings.HasSuffix(configPath, ".yml") {
		err = yaml.Unmarshal(content, &config)
	} else {
		return nil, fmt.Errorf("unsupported config file format. Only JSON and YAML are supported")
	}
	if err != nil {
		return nil, err
	}

	return &config, nil
}

// Parse the data payload based on the specified type
func parseData(data string, dataType string) (interface{}, error) {
	switch dataType {
	case "json":
		var message Message
		err := json.Unmarshal([]byte(data), &message)
		return message, err
	case "yaml", "yml":
		var message Message
		err := yaml.Unmarshal([]byte(data), &message)
		return message, err
	case "csv":
		return strings.Split(data, ","), nil
	case "tsv":
		return strings.Split(data, "\t"), nil
	case "ssv":
		return strings.Fields(data), nil
	default:
		return data, nil
	}
}

// Replace mustache syntax in string with corresponding data values
func replaceMustache(str string, data Message) string {
	for key, value := range data {
		str = strings.ReplaceAll(str, "{{"+key+"}}", fmt.Sprintf("%v", value))
	}
	return str
}

// Produce a message to Kafka
func produceMessage(producer *kafka.Producer, topic string, key string, header string, value string) (*kafka.Message, error) {
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(key),
		Headers:        []kafka.Header{{Key: header, Value: []byte(value)}},
		Value:          []byte(value),
	}

	return producer.Produce(msg, nil)
}

func main() {
	// Parse command line arguments
	configPath := flag.String("config", "", "Path to configuration file (JSON or YAML)")
	data := flag.String("data", "", "Message data")
	dataType := flag.String("type", "json", "Type of the message data (default: json)")
	topic := flag.String("topic", "", "Kafka topic")
	key := flag.String("key", "", "Key for the Kafka message")
	header := flag.String("header", "", "Header for the Kafka message")
	flag.Parse()

	// Validate required arguments
	if *configPath == "" || *data == "" || *topic == "" {
		flag.Usage()
		os.Exit(1)
	}

	// Load configuration from file
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Fatal("Failed to load configuration:", err)
	}

	// Set values from configuration if not provided as command line arguments
	if *key == "" && config.Key != "" {
		*key = config.Key
	}
	if *header == "" && config.Header != "" {
		*header = config.Header
	}
	if *dataType == "json" && config.Type != "" {
		*dataType = config.Type
	}

	// Create Kafka producer
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.BootstrapServers,
		"security.protocol": config.SecurityProtocol,
		"ssl.ca.location":   config.SSLCAFile,
		"ssl.certificate.location": config.SSLCertFile,
		"ssl.key.location":         config.SSLKeyFile,
	})
	if err != nil {
		log.Fatal("Failed to create Kafka producer:", err)
	}

	// Prepare message data
	messageData, err := parseData(*data, *dataType)
	if err != nil {
		log.Fatal("Failed to parse message data:", err)
	}

	// Produce messages
	if config.Multi && isArray(messageData) {
		messages, ok := messageData.([]interface{})
		if !ok {
			log.Fatal("Invalid message data in multi-mode")
		}

		for _, msg := range messages {
			message, ok := msg.(Message)
			if !ok {
				log.Println("Ignoring non-object item in multi-mode")
				continue
			}

			topicName := replaceMustache(*topic, message)
			keyValue := replaceMustache(*key, message)
			headerValue := replaceMustache(*header, message)

			msgValue, err := json.Marshal(message)
			if err != nil {
				log.Fatal("Failed to marshal message:", err)
			}

			_, _, err = produceMessage(producer, topicName, keyValue, headerValue, string(msgValue))
			if err != nil {
				log.Fatal("Failed to produce message:", err)
			}
			log.Printf("Produced message to topic: %s", topicName)
		}
	} else {
		topicName := replaceMustache(*topic, messageData.(Message))
		keyValue := replaceMustache(*key, messageData.(Message))
		headerValue := replaceMustache(*header, messageData.(Message))

		msgValue, err := json.Marshal(messageData)
		if err != nil {
			log.Fatal("Failed to marshal message:", err)
		}

		_, _, err = produceMessage(producer, topicName, keyValue, headerValue, string(msgValue))
		if err != nil {
			log.Fatal("Failed to produce message:", err)
		}
		log.Printf("Produced message to topic: %s", topicName)
	}

	producer.Close()
}

// Helper function to check if a variable is an array
func isArray(data interface{}) bool {
	_, ok := data.([]interface{})
	return ok
}
