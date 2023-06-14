const fs = require('fs');
const { Kafka } = require('kafkajs');
const YAML = require('yaml');

// Function to load configuration from JSON or YAML file
function loadConfig(configPath) {
  const fileContent = fs.readFileSync(configPath, 'utf8');
  if (configPath.endsWith('.json')) {
    return JSON.parse(fileContent);
  } else if (configPath.endsWith('.yaml') || configPath.endsWith('.yml')) {
    return YAML.parse(fileContent);
  } else {
    throw new Error('Unsupported config file format. Only JSON and YAML are supported.');
  }
}

// Function to parse the data payload based on the specified type
function parseData(data, dataType) {
  if (dataType === 'json') {
    return JSON.parse(data);
  } else if (dataType === 'yaml' || dataType === 'yml') {
    return YAML.parse(data);
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
function replaceMustache(string, data) {
  for (const [key, value] of Object.entries(data)) {
    string = string.replace('{{' + key + '}}', value);
  }
  return string;
}

// Function to produce a message to Kafka
async function produceMessage(producer, topic, key, header, value) {
  const message = {
    key: key,
    headers: [{ key: header, value: Buffer.from(value) }],
    value: Buffer.from(value)
  };
  const result = await producer.send({ topic, messages: [message] });
  return result;
}

// Parse command line arguments
const args = require('yargs')
  .option('config', {
    alias: 'c',
    describe: 'Path to configuration file (JSON or YAML)',
    type: 'string',
    demandOption: true
  })
  .option('data', {
    alias: 'd',
    describe: 'Message data',
    type: 'string',
    demandOption: true
  })
  .option('type', {
    describe: 'Type of the message data (default: json)',
    type: 'string',
    default: 'json'
  })
  .option('topic', {
    alias: 't',
    describe: 'Kafka topic',
    type: 'string',
    demandOption: true
  })
  .option('key', {
    describe: 'Key for the Kafka message',
    type: 'string'
  })
  .option('header', {
    describe: 'Header for the Kafka message',
    type: 'string'
  })
  .help()
  .argv;

// Load configuration from file
const config = loadConfig(args.config);

// Set values from configuration if not provided as command line arguments
if (!args.key && config.key) {
  args.key = config.key;
}
if (!args.header && config.header) {
  args.header = config.header;
}
if (config.type) {
  args.type = config.type;
}

// Create Kafka producer
const kafka = new Kafka({
  brokers: config.bootstrap_servers,
  ssl: {
    caFile: config.ssl_cafile,
    certFile: config.ssl_certfile,
    keyFile: config.ssl_keyfile
  }
});
const producer = kafka.producer();

// Prepare message data
const data = parseData(args.data, args.type);

// Produce messages
async function produceMessages() {
  await producer.connect();
  if (args.multi && Array.isArray(data)) {
    for (const item of data) {
      if (typeof item === 'object') {
        const topic = replaceMustache(args.topic, item);
        const key = replaceMustache(args.key, item) || null;
        const header = replaceMustache(args.header, item) || null;
        const result = await produceMessage(producer, topic, key, header, JSON.stringify(item));
        console.log(`Produced message to topic: ${result.topic}, partition: ${result.partition}, offset: ${result.offset}`);
      } else {
        console.log('Ignoring non-object item in multi-mode.');
      }
    }
  } else {
    const topic = replaceMustache(args.topic, data);
    const key = replaceMustache(args.key, data) || null;
    const header = replaceMustache(args.header, data) || null;
    const result = await produceMessage(producer, topic, key, header, JSON.stringify(data));
    console.log(`Produced message to topic: ${result.topic}, partition: ${result.partition}, offset: ${result.offset}`);
  }
  await producer.disconnect();
}

produceMessages().catch((error) => {
  console.error('An error occurred:', error);
  process.exit(1);
});
