import argparse
import json
import yaml
import xml.etree.ElementTree as ET
from kafka import KafkaProducer

# Function to load configuration from JSON or YAML file
def load_config(config_path):
    with open(config_path, 'r') as f:
        if config_path.endswith('.json'):
            return json.load(f)
        elif config_path.endswith('.yaml') or config_path.endswith('.yml'):
            return yaml.safe_load(f)
        else:
            raise ValueError("Unsupported config file format. Only JSON and YAML are supported.")

# Function to parse the data payload based on the specified type
def parse_data(data, data_type):
    if data_type == 'json':
        return json.loads(data)
    elif data_type == 'yaml' or data_type == 'yml':
        return yaml.safe_load(data)
    elif data_type == 'csv':
        return data.split(',')
    elif data_type == 'tsv':
        return data.split('\t')
    elif data_type == 'ssv':
        return data.split()
    elif data_type == 'xml':
        return ET.fromstring(data)
    else:
        return data

# Function to replace mustache syntax in string with corresponding data values
def replace_mustache(string, data):
    for key, value in data.items():
        string = string.replace('{{' + key + '}}', value)
    return string

# Function to produce a message to Kafka
def produce_message(producer, topic, key, header, value):
    return producer.send(topic, key=key.encode('utf-8'), headers=[(header, value.encode('utf-8'))], value=value.encode('utf-8'))

# Parse command line arguments
parser = argparse.ArgumentParser(description='Produce messages to a Kafka topic')
parser.add_argument('--config', type=str, required=True, help='Path to configuration file (JSON or YAML)')
parser.add_argument('--data', type=str, required=True, help='Message data')
parser.add_argument('--type', type=str, default='json', help='Type of the message data (default: json)')
parser.add_argument('--topic', type=str, required=True, help='Kafka topic')
parser.add_argument('--key', type=str, default=None, help='Key for the Kafka message')
parser.add_argument('--header', type=str, default=None, help='Header for the Kafka message')
args = parser.parse_args()

# Load configuration from file
config = load_config(args.config)

# Set values from configuration if not provided as command line arguments
if args.key is None and 'key' in config:
    args.key = config['key']
if args.header is None and 'header' in config:
    args.header = config['header']
if 'type' in config:
    args.type = config['type']
if 'multi' in config and config['multi']:
    args.multi = True

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=config['bootstrap_servers'],
    security_protocol=config['security_protocol'],
    ssl_cafile=config['ssl_cafile'],
    ssl_certfile=config['ssl_certfile'],
    ssl_keyfile=config['ssl_keyfile'],
)

# Prepare message data
data = parse_data(args.data, args.type)

# Produce messages
if args.multi and isinstance(data, list):
    for item in data:
        if isinstance(item, dict):
            topic = replace_mustache(args.topic, item)
            key = replace_mustache(args.key, item) if args.key else None
            header = replace_mustache(args.header, item) if args.header else None
            future = produce_message(producer, topic, key, header, json.dumps(item))
            print(f"Produced message to topic: {future.topic()}, partition: {future.partition()}, offset: {future.offset()}")
        else:
            print("Ignoring non-dict item in multi-mode.")
else:
    topic = replace_mustache(args.topic, data)
    key = replace_mustache(args.key, data) if args.key else None
    header = replace_mustache(args.header, data) if args.header else None
    future = produce_message(producer, topic, key, header, json.dumps(data))
    print(f"Produced message to topic: {future.topic()}, partition: {future.partition()}, offset: {future.offset()}")

# Flush and close the producer
producer.flush()
producer.close()
