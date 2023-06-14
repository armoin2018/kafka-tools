import sys
import yaml
import argparse
from confluent_kafka import Producer, Consumer, KafkaException, TopicPartition
from datetime import datetime

def load_configuration(config_file):
    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)
    return config

def configure_producer(config):
    producer_config = config['producer']

    security_protocol = producer_config.get('security_protocol')
    ssl_config = producer_config.get('ssl')
    sasl_plain_config = producer_config.get('sasl_plain')

    conf = {
        'bootstrap.servers': producer_config['brokers'],
        'security.protocol': security_protocol
    }

    if security_protocol == 'ssl':
        conf['ssl.ca.location'] = ssl_config['ca_location']
        conf['ssl.certificate.location'] = ssl_config['certificate_location']
        conf['ssl.key.location'] = ssl_config['key_location']
    elif security_protocol == 'sasl_plain':
        conf['sasl.username'] = sasl_plain_config['username']
        conf['sasl.password'] = sasl_plain_config['password']

    producer = Producer(conf)
    return producer

def configure_consumer(config):
    consumer_config = config['consumer']

    security_protocol = consumer_config.get('security_protocol')
    ssl_config = consumer_config.get('ssl')
    sasl_plain_config = consumer_config.get('sasl_plain')
    
    conf = {
        'bootstrap.servers': consumer_config['brokers'],
        'group.id': consumer_config['consumer_group'],
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'enable.partition.eof': True,
        'session.timeout.ms': 6000
    }
    
    if security_protocol == 'ssl':
        conf['security.protocol'] = security_protocol
        conf['ssl.ca.location'] = ssl_config['ca_location']
        conf['ssl.certificate.location'] = ssl_config['certificate_location']
        conf['ssl.key.location'] = ssl_config['key_location']
    elif security_protocol == 'sasl_plain':
        conf['security.protocol'] = security_protocol
        conf['sasl.username'] = sasl_plain_config['username']
        conf['sasl.password'] = sasl_plain_config['password']

    consumer = Consumer(conf)
    return consumer

def consume_and_produce_messages(config):
    producer = configure_producer(config)
    consumer = configure_consumer(config)

    for topic_config in config['topic_configs']:
        source_topic = topic_config['source']['topic']
        source_partition = topic_config['source'].get('partition')
        destination_topic = topic_config['destination'].get('topic', source_topic)
        destination_partition = topic_config['destination'].get('partition', source_partition)
        
        if source_partition is not None:
            # Consume messages from a specific partition
            consumer.assign([TopicPartition(source_topic, source_partition)])
        else:
            # Consume messages from all partitions
            consumer.subscribe([source_topic])
        
        if source_partition is None and ('startOffset' in topic_config['source'] or 'endOffset' in topic_config['source']):
            print(f"Invalid configuration for topic '{source_topic}'. Offset range can only be specified when source partition is provided.")
            continue    
            
        if 'startOffset' in topic_config['source'] and 'endOffset' in topic_config['source']:
            # Consume messages based on offset range
            source_start_offset = topic_config['source']['startOffset']
            source_end_offset = topic_config['source']['endOffset']
            consumer.assign([TopicPartition(source_topic, source_partition, source_start_offset)])
        elif 'startTime' in topic_config['source'] and 'endTime' in topic_config['source']:
            # Consume messages based on timestamp range
            source_start_time = datetime.strptime(topic_config['source']['startTime'], '%Y-%m-%d %H:%M:%S')
            source_end_time = datetime.strptime(topic_config['source']['endTime'], '%Y-%m-%d %H:%M:%S')
            consumer.assign([TopicPartition(source_topic, source_partition, source_start_time)])
        else:
            print(f"Invalid configuration for topic '{source_topic}'. Specify either offset range or timestamp range.")
            continue
        
        # Start consuming messages
        while True:
            try:
                msg = consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, stop consuming
                        break
                    else:
                        # Handle Kafka error
                        raise KafkaException(msg.error())
                
                # Copy message key and headers if required
                key = msg.key() if topic_config['source']['copyKey'] else None
                headers = msg.headers() if topic_config['source']['copyHeader'] else None
                
                # Determine the destination partition
                destination_partition = destination_partition or msg.partition()
                
                # Produce message to destination topic and partition
                producer.produce(destination_topic, value=msg.value(), key=key, headers=headers, partition=destination_partition)

                # Commit the consumed message offset
                consumer.commit(msg)
                
            except KeyboardInterrupt:
                # Stop consuming on keyboard interrupt
                break
    
    # Close consumer and producer
    consumer.close()
    producer.flush()

# Rest of the code remains the same...
def print_help():
    help_text = """
Usage: python kafka-replay.py <config_file>

Options:
    --help  Show this help message and exit.
    """
    print(help_text)

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "--help":
        print_help()
        sys.exit(0)
    
    config_file = sys.argv[1]
    config = load_configuration(config_file)
    
    # Consume and produce messages
    consume_and_produce_messages(config)
