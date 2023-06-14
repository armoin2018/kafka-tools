import argparse
import json
import yaml
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime

def consume_messages(config, topics, show_options, output, output_file_pattern, output_type, offset, timestamp, group,
                     timestamp_format):
    # Load Kafka configuration from JSON or YAML file
    if config.endswith('.json'):
        with open(config) as file:
            kafka_config = json.load(file)
    elif config.endswith('.yaml') or config.endswith('.yml'):
        with open(config) as file:
            kafka_config = yaml.safe_load(file)
    else:
        # Assume config is a space-separated key-value string
        kafka_config = dict(item.split('=') for item in config.split())

    # Create Kafka consumer
    consumer = KafkaConsumer(bootstrap_servers=kafka_config['bootstrap_servers'],
                             security_protocol=kafka_config['security_protocol'],
                             ssl_cafile=kafka_config['ssl_cafile'],
                             ssl_certfile=kafka_config['ssl_certfile'],
                             ssl_keyfile=kafka_config['ssl_keyfile'],
                             ssl_check_hostname=kafka_config.get('ssl_check_hostname', True),
                             group_id=group)

    partitions = []
    for topic in topics:
        if ':' in topic:
            topic_name, partition_num = topic.split(':')
            partition = int(partition_num)
            partitions.append(TopicPartition(topic=topic_name, partition=partition))
        else:
            partitions.append(topic)

    consumer.assign(partitions)

    if offset:
        # Seek to the specified offset for each partition
        for partition in partitions:
            consumer.seek(partition, offset)
    elif timestamp:
        # Convert timestamp to milliseconds
        timestamp_ms = int(timestamp * 1000)
        # Seek to the specified timestamp for each partition
        for partition in partitions:
            consumer.seek(partition, timestamp_ms)

    # Iterate over messages and extract desired information
    messages = []
    for message in consumer:
        data = {}
        if 'topic' in show_options:
            data['topic'] = message.topic
        if 'partition' in show_options:
            data['partition'] = message.partition
        if 'offset' in show_options:
            data['offset'] = message.offset
        if 'timestamp' in show_options:
            data['timestamp'] = datetime.fromtimestamp(message.timestamp / 1000).strftime(timestamp_format)
        if 'key' in show_options:
            data['key'] = message.key.decode('utf-8')
        if 'header' in show_options:
            data['header'] = dict(message.headers)
        data['message'] = message.value.decode('utf-8')
        messages.append(data)

        # Write message to output file if specified
        if output:
            timestamp_parts = {
                'Y': '%Y',
                'm': '%m',
                'M': '%M',
                'd': '%d',
                'H': '%H',
                'S': '%S',
                'MS': '{:03d}'.format(int(datetime.fromtimestamp(message.timestamp / 1000).strftime('%f')[:3]))
            }

            file_name = output_file_pattern
            for part, format_str in timestamp_parts.items():
                file_name = file_name.replace('{{' + part + '}}', format_str)

            file_name = file_name.replace('{{topic}}', message.topic)\
                .replace('{{partition}}', str(message.partition))\
                .replace('{{offset}}', str(message.offset))\
                .replace('{{extension}}', output_type)

            file_path = f"{output}/{file_name}"
            with open(file_path, 'a') as file:
                if output_type == 'json':
                    file.write(f"{json.dumps(data)}\n")
                elif output_type in ['yaml', 'yml']:
                    file.write(f"{yaml.dump(data)}\n")
                elif output_type == 'xml':
                    xml_data = convert_to_xml(data)
                    file.write(f"{xml_data}\n")
                elif output_type == 'csv':
                    csv_data = convert_to_csv(data)
                    file.write(f"{csv_data}\n")
                elif output_type == 'tsv':
                    tsv_data = convert_to_tsv(data)
                    file.write(f"{tsv_data}\n")
                elif output_type == 'ssv':
                    ssv_data = convert_to_ssv(data)
                    file.write(f"{ssv_data}\n")
                else:
                    raise ValueError(f"Unsupported output type: {output_type}")

    return messages

def convert_to_xml(data):
    # Convert data dictionary to XML format
    xml_data = "<message>\n"
    for key, value in data.items():
        if key == 'header':
            xml_data += "  <header>\n"
            for h_key, h_value in value.items():
                xml_data += f"    <{h_key}>{h_value}</{h_key}>\n"
            xml_data += "  </header>\n"
        else:
            xml_data += f"  <{key}>{value}</{key}>\n"
    xml_data += "</message>"
    return xml_data

def convert_to_csv(data):
    # Convert data dictionary to CSV format
    csv_data = ''
    for key, value in data.items():
        if key == 'header':
            for h_key, h_value in value.items():
                csv_data += f"{h_key},{h_value}\n"
        else:
            csv_data += f"{key},{value}\n"
    return csv_data

def convert_to_tsv(data):
    # Convert data dictionary to TSV format
    tsv_data = ''
    for key, value in data.items():
        if key == 'header':
            for h_key, h_value in value.items():
                tsv_data += f"{h_key}\t{h_value}\n"
        else:
            tsv_data += f"{key}\t{value}\n"
    return tsv_data

def convert_to_ssv(data):
    # Convert data dictionary to SSV format
    ssv_data = ''
    for key, value in data.items():
        if key == 'header':
            for h_key, h_value in value.items():
                ssv_data += f"{h_key} {h_value}\n"
        else:
            ssv_data += f"{key} {value}\n"
    return ssv_data

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Kafka message consumer')
    parser.add_argument('--config', required=True, help='Kafka connection and security configuration')
    parser.add_argument('--topics', required=True, help='Kafka topics to consume from')
    parser.add_argument('--show', help='Comma-separated list of options to show (key, header, partition, topic, offset, timestamp)')
    parser.add_argument('--output', help='Output folder for messages')
    parser.add_argument('--outputFilePattern', default='{{topic}}-{{partition}}-{{offset}}.{{extension}}',
                        help='Output file name pattern using mustache syntax')
    parser.add_argument('--type', default='json',
                        choices=['text', 'json', 'yaml', 'yml', 'xml', 'csv', 'tsv', 'ssv'],
                        help='Output type')
    parser.add_argument('--offset', type=int, help='Start consuming from the specified offset')
    parser.add_argument('--timestamp', type=float, help='Start consuming from the specified timestamp')
    parser.add_argument('--group', help='Consumer group name')
    parser.add_argument('--timestampFormat', default='%Y-%m-%d %H:%M:%S.{{MS}}',
                        help='Timestamp format for output file name using strftime syntax')
    args = parser.parse_args()

    show_options = []
    if args.show:
        show_options = args.show.split(',')

    messages = consume_messages(args.config, args.topics.split(','), show_options, args.output,
                                args.outputFilePattern, args.type, args.offset, args.timestamp, args.group,
                                args.timestampFormat)

    if args.type == 'json':
        print(json.dumps(messages))
    elif args.type in ['yaml', 'yml']:
        print(yaml.dump_all(messages, default_flow_style=False))
    else:
        print("Unsupported output type")
