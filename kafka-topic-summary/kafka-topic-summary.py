import json
from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaException

def load_config():
    with open('config.json') as config_file:
        config_data = json.load(config_file)
    return config_data

def count_messages_for_day(config, topic, consumer_group, date):
    consumer_config = {
        'bootstrap.servers': config['kafka']['bootstrap.servers'],
        'group.id': consumer_group,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    consumer = Consumer(consumer_config)
    
    consumer.subscribe([topic])
    
    total_count = 0
    total_size = 0
    
    try:
        while True:
            message = consumer.poll(timeout=1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            
            message_time = datetime.utcfromtimestamp(message.timestamp() / 1000)
            if message_time.date() == date:
                total_count += 1
                total_size += len(message.value() or b'')
            
            consumer.commit(asynchronous=False)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    
    return {'count': total_count, 'size': total_size, 'average_size': total_size / total_count if total_count > 0 else 0}

def generate_report(config):
    topics = config['topics']
    consumer_groups = config['consumerGroups']

    for topic in topics:
        for consumer_group in consumer_groups:
            timestamps = get_timestamps_for_topic(config, topic)
            print(f'Topic: {topic}, Consumer Group: {consumer_group}')
            print('--------------------------')
            for timestamp in timestamps:
                result = count_messages_for_day(config, topic, consumer_group, timestamp.date())
                print(f"Date: {timestamp.date().isoformat()}")
                print(f"Total Count: {result['count']}")
                print(f"Total Size: {result['size']}")
                print(f"Average Message Size: {result['average_size']}")
                print('--------------------------')

def get_timestamps_for_topic(config, topic):
    consumer_config = {
        'bootstrap.servers': config['kafka']['bootstrap.servers'],
        'group.id': 'timestamp_group',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
    }
    consumer = Consumer(consumer_config)
    
    consumer.subscribe([topic])
    partitions = consumer.assignment()
    
    timestamps = []
    
    try:
        for partition in partitions:
            low, high = consumer.get_watermark_offsets(partition)
            start_offset = consumer.offsets_for_times([(partition, low)], timeout=1.0)[0][1].offset
            end_offset = consumer.offsets_for_times([(partition, high)], timeout=1.0)[0][1].offset
            
            if start_offset is None or end_offset is None:
                continue
            
            consumer.seek(partition, start_offset)
            while consumer.position(partition) < end_offset:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                timestamps.append(datetime.utcfromtimestamp(msg.timestamp() / 1000))
                consumer.commit(asynchronous=False)
            consumer.unsubscribe()
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
    
    return timestamps

config = load_config()
generate_report(config)
