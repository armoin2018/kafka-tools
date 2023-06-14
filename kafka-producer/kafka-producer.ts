import * as fs from 'fs';
import { Kafka, Producer, Message } from 'kafkajs';
import yaml from 'js-yaml';

interface Configuration {
  bootstrap_servers: string;
  security_protocol: string;
  ssl_cafile: string;
  ssl_certfile: string;
  ssl_keyfile: string;
  key?: string;
  header?: string;
  type?: string;
  multi?: boolean;
}

// Function to load configuration from JSON or YAML file
function loadConfig(configPath: string): Configuration {
  const fileContent = fs.readFileSync(configPath, 'utf8');
  if (configPath.endsWith('.json')) {
    return JSON.parse(fileContent);
  } else if (configPath.endsWith('.yaml') || configPath.endsWith('.yml')) {
    return yaml.safeLoad(fileContent) as Configuration;
  } else {
    throw new Error('Unsupported config file format. Only JSON and YAML are supported.');
  }
}

// Function to parse the data payload based on the specified type
function parseData(data: string, dataType: string): unknown {
  if (dataType === 'json') {
    return JSON.parse(data);
  } else if (dataType === 'yaml' || dataType === 'yml') {
    return yaml.safeLoad(data);
  } else if (dataType === 'csv') {
    return data.split(',');
  } else if (dataType === 'tsv') {
    return data.split('\t');
  } else if (dataType === 'ssv') {
    return data.split(' ');
  } else {
    return data;
  }
}

// Function to replace mustache syntax in string with corresponding data values
function replaceMustache(str: string, data: { [key: string]: unknown }): string {
  for (const [key, value] of Object.entries(data)) {
    str = str.replace(`{{${key}}}`, String(value));
  }
  return str;
}

// Function to produce a message to Kafka
async function produceMessage(
  producer: Producer,
  topic: string,
  key: string | null,
  header: string | null,
  value: string
): Promise<Message> {
  return producer.send({
    topic,
    messages: [{ key: key ? String(key) : null, headers: [{ key: header || '', value: String(value) }], value }],
  });
}

// Main function
async function main() {
  const args = require('yargs')
    .option('config', {
      alias: 'c',
      describe: 'Path to configuration file (JSON or YAML)',
      type: 'string',
      demandOption: true,
    })
    .option('data', {
      alias: 'd',
      describe: 'Message data',
      type: 'string',
      demandOption: true,
    })
    .option('type', {
      describe: 'Type of the message data (default: json)',
      type: 'string',
      default: 'json',
    })
    .option('topic', {
      alias: 't',
      describe: 'Kafka topic',
      type: 'string',
      demandOption: true,
    })
    .option('key', {
      describe: 'Key for the Kafka message',
      type: 'string',
    })
    .option('header', {
      describe: 'Header for the Kafka message',
      type: 'string',
    })
    .help().argv;

  const config: Configuration = loadConfig(args.config);
  const data: unknown = parseData(args.data, args.type);

  if (config.multi && Array.isArray(data)) {
    const kafka = new Kafka({
      brokers: config.bootstrap_servers,
      ssl: {
        caFile: config.ssl_cafile,
        certFile: config.ssl_certfile,
        keyFile: config.ssl_keyfile,
      },
    });
    const producer = kafka.producer();

    await producer.connect();

    for (const item of data) {
      if (typeof item === 'object') {
        const topic = replaceMustache(args.topic, item);
        const key = args.key ? replaceMustache(args.key, item) : null;
        const header = args.header ? replaceMustache(args.header, item) : null;
        const value = JSON.stringify(item);

        const result = await produceMessage(producer, topic, key, header, value);
        console.log(`Produced message to topic: ${result.topic}, partition: ${result.partition}, offset: ${result.offset}`);
      } else {
        console.log('Ignoring non-object item in multi-mode.');
      }
    }

    await producer.disconnect();
  } else {
    const kafka = new Kafka({
      brokers: config.bootstrap_servers,
      ssl: {
        caFile: config.ssl_cafile,
        certFile: config.ssl_certfile,
        keyFile: config.ssl_keyfile,
      },
    });
    const producer = kafka.producer();

    await producer.connect();

    const topic = replaceMustache(args.topic, data as { [key: string]: unknown });
    const key = args.key ? replaceMustache(args.key, data as { [key: string]: unknown }) : null;
    const header = args.header ? replaceMustache(args.header, data as { [key: string]: unknown }) : null;
    const value = JSON.stringify(data);

    const result = await produceMessage(producer, topic, key, header, value);
    console.log(`Produced message to topic: ${result.topic}, partition: ${result.partition}, offset: ${result.offset}`);

    await producer.disconnect();
  }
}

main().catch((error) => {
  console.error('An error occurred:', error);
  process.exit(1);
});
