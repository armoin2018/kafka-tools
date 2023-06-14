import * as fs from 'fs';
import { Kafka, EachMessagePayload, Admin } from 'kafkajs';

interface MessageData {
  key?: string | null;
  header?: { [key: string]: string };
  partition?: number;
  topic?: string;
  offset?: string;
  timestamp?: string;
  message: string;
}

async function consumeMessages(
  config: any,
  topics: string[],
  showOptions: string[],
  output: string | undefined,
  outputFilePattern: string,
  outputType: string,
  offset: number | undefined,
  timestamp: number | undefined,
  group: string | undefined
): Promise<MessageData[]> {
  const kafka = new Kafka(config);
  const consumer = kafka.consumer({ groupId: group });

  const partitions = topics.map(topic => {
    if (topic.includes(':')) {
      const [topicName, partition] = topic.split(':');
      return { topic: topicName, partition: parseInt(partition) };
    } else {
      return topic;
    }
  });

  await consumer.connect();
  await consumer.subscribe({ topic: partitions });

  if (offset) {
    for (const partition of partitions) {
      await consumer.seek({ topic: partition.topic, partition: partition.partition, offset });
    }
  } else if (timestamp) {
    for (const partition of partitions) {
      await consumer.seek({ topic: partition.topic, partition: partition.partition, timestamp });
    }
  }

  const messages: MessageData[] = [];

  await consumer.run({
    eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
      const data: MessageData = {};
      if (showOptions.includes('key')) {
        data.key = message.key?.toString() || null;
      }
      if (showOptions.includes('header')) {
        data.header = Object.fromEntries(
          message.headers?.map(({ key, value }) => [key.toString(), value.toString()]) || []
        );
      }
      if (showOptions.includes('partition')) {
        data.partition = partition;
      }
      if (showOptions.includes('topic')) {
        data.topic = topic;
      }
      if (showOptions.includes('offset')) {
        data.offset = message.offset?.toString();
      }
      if (showOptions.includes('timestamp')) {
        data.timestamp = message.timestamp?.toString();
      }
      data.message = message.value.toString();
      messages.push(data);

      if (output) {
        const timestampParts: Record<string, string> = {
          Y: 'YYYY',
          m: 'MM',
          M: 'mm',
          d: 'DD',
          H: 'HH',
          S: 'ss',
          MS: 'SSS',
        };

        let fileName = outputFilePattern;
        for (const part in timestampParts) {
          fileName = fileName.replace('{{' + part + '}}', timestampParts[part]);
        }
        fileName = fileName.replace('{{topic}}', topic)
          .replace('{{partition}}', partition.toString())
          .replace('{{offset}}', message.offset?.toString() || '')
          .replace('{{extension}}', outputType);

        const filePath = `${output}/${fileName}`;
        const dataToWrite = outputType === 'json' ? JSON.stringify(data) + '\n' : data.message + '\n';
        fs.appendFileSync(filePath, dataToWrite);
      }
    },
  });

  return messages;
}

async function run() {
  const args = require('minimist')(process.argv.slice(2));
  const { config, topics, show, output, outputFilePattern, type, offset, timestamp, group, timestampFormat } = args;

  const showOptions = show ? show.split(',') : [];
  const outputType = type || 'json';

  const configContent = fs.readFileSync(config, 'utf8');
  const kafkaConfig = JSON.parse(configContent);

  const messages = await consumeMessages(
    kafkaConfig,
    topics.split(','),
    showOptions,
    output,
    outputFilePattern,
    outputType,
    offset,
    timestamp,
    group
  );

  if (outputType === 'json') {
    console.log(JSON.stringify(messages));
  } else if (outputType === 'yaml' || outputType === 'yml') {
    const yamlData = messages.map(message => YAML.stringify(message)).join('---\n');
    console.log(yamlData);
  } else {
    console.log('Unsupported output type');
  }
}

run().catch(console.error);
