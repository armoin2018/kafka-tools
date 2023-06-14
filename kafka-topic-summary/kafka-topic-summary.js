const fs = require('fs');
const Kafka = require('node-rdkafka');

const configFile = 'config.json';

const loadConfig = () => {
    const configData = fs.readFileSync(configFile);
    return JSON.parse(configData);
};

const createConsumer = (config) => {
    const consumer = new Kafka.KafkaConsumer(config.consumer);

    consumer.setKafkaConfig(config.kafka);

    consumer.on('event.error', (err) => {
        console.error('Error:', err);
        consumer.disconnect();
    });

    return consumer;
};

const createProducer = (config) => {
    const producer = new Kafka.Producer(config.producer);

    producer.setKafkaConfig(config.kafka);

    producer.on('event.error', (err) => {
        console.error('Error:', err);
        producer.disconnect();
    });

    return producer;
};

const getTopicMetadata = (config, callback) => {
    const consumer = createConsumer(config);

    consumer.connect();

    consumer.on('ready', () => {
        const metadata = consumer.getMetadata();
        consumer.disconnect();
        callback(metadata);
    });
};

const getTimestampsForTopic = (config, topic) => {
    return new Promise((resolve, reject) => {
        getTopicMetadata(config, (metadata) => {
            const topicMetadata = metadata.topics.find((t) => t.name === topic);

            if (!topicMetadata) {
                reject(new Error(`Topic '${topic}' not found in metadata`));
                return;
            }

            const timestamps = [];
            const partitions = topicMetadata.partitions;

            const consumer = createConsumer(config);

            consumer.connect();

            consumer.on('ready', () => {
                const fetchTimestamps = () => {
                    let completedPartitions = 0;

                    partitions.forEach((partition) => {
                        consumer.offsetsForTime(topic, partition.id, -1, 1, (err, offsets) => {
                            if (err) {
                                reject(err);
                                return;
                            }

                            if (offsets && offsets.length > 0) {
                                const offset = offsets[0];
                                if (offset.offset !== Kafka.OFFSET_INVALID) {
                                    const timestamp = new Date(offset.timestamp * 1000);
                                    timestamps.push(timestamp);
                                }
                            }

                            completedPartitions++;
                            if (completedPartitions === partitions.length) {
                                consumer.disconnect();
                                resolve(timestamps);
                            }
                        });
                    });
                };

                fetchTimestamps();
            });

            consumer.on('event.error', (err) => {
                reject(err);
                consumer.disconnect();
            });
        });
    });
};

const countMessagesForDay = (config, topic, consumerGroup, date) => {
    return new Promise((resolve, reject) => {
        const consumerConfig = Object.assign({}, config.consumer, { 'group.id': consumerGroup });
        const consumer = createConsumer({ consumer: consumerConfig, kafka: config.kafka });

        consumer.connect();

        consumer.on('ready', () => {
            consumer.subscribe([topic]);
            consumer.consume();
        });

        let totalCount = 0;
        let totalSize = 0;

        consumer.on('data', (message) => {
            const messageTime = new Date(message.timestamp);
            if (messageTime >= date && messageTime < new Date(date.getTime() + 24 * 60 * 60 * 1000)) {
                totalCount++;
                totalSize += message.value ? message.value.length : 0;
            }
        });

        // ...

        consumer.on('event.error', (err) => {
            reject(err);
            consumer.disconnect();
        });

        consumer.on('disconnected', () => {
            const averageSize = totalCount > 0 ? totalSize / totalCount : 0;
            resolve({ count: totalCount, size: totalSize, averageSize });
        });
    });
};

const generateReport = async (config) => {
    const topics = config.topics;
    const consumerGroups = config.consumerGroups;

    for (const topic of topics) {
        for (const consumerGroup of consumerGroups) {
            const timestamps = await getTimestampsForTopic(config, topic);
            console.log(`Topic: ${topic}, Consumer Group: ${consumerGroup}`);
            console.log('--------------------------');
            for (const timestamp of timestamps) {
                const result = await countMessagesForDay(config, topic, consumerGroup, timestamp);
                console.log(`Date: ${timestamp.toISOString().slice(0, 10)}`);
                console.log(`Total Count: ${result.count}`);
                console.log(`Total Size: ${result.size}`);
                console.log(`Average Message Size: ${result.averageSize}`);
                console.log('--------------------------');
            }
        }
    }
};

const config = loadConfig();
generateReport(config);
