import argparse, json, yaml, configparser, csv
from kafka import KafkaProducer

# Function to load configuration from JSON or YAML file

def parseJSON(inContents):
    try:
        return json.loads(inContents)
    except json.JSONDecodeError:
        pass
    return inContents

def parseYAML(inContents):
    try:
        return yaml.safe_load(inContents)
    except yaml.YAMLError:
        pass
    return inContents

def parseINI(inContents):
    try:
        config = configparser.ConfigParser()
        config.read_string(inContents)
        return config
    except configparser.Error:
        pass
    return inContents

def parseXML(inContents):
    try:
        return ET.fromstring(inContents)
    except ET.ParseError:
        pass
    return inContents

def parseCSV(inFile,csvHeaders=None):
    myResponse = []
    try:
        with open(inFile, newline='') as csvFile:
            reader = csv.reader(csvFile)
            if headers == None:
                headers = next(reader)
            else:
                headers = csvHeaders.split(",")
            
            for row in reader:
                data_row = {header: value for header, value in zip(headers, row)}
                myResponse.append(data_row)
    except FileNotFoundError:
        pass
    except Exception as e:
        print("Error reading file: ",e)    
    return myResponse

def parseFile(inFile):
    try:
        with open(inFile,'r') as file:
            return file.read()
    except FileNotFoundError:
        pass
    except Exception as e:
        print("Error reading file: ",e)
    return None

def parseInput(inContents,inType=None,CSVheaders=None):
    myResponse=inContents
    if inContents.startswith("@"):
        file_in=inContents[1:]
        inType=file_in.split(".")[-1]
        if inType == "csv":
            myResponse=parseCSV(file_in,csvHeaders)
        else: 
            myResponse=parseFile(file_in)

    if inType != "txt" and inType != "text":
        if inType == "json":
            myResponse=parseJSON(myResponse)
        elif inType == "yaml" or inType == "yml":
            myResponse=parseYAML(myResponse)
        elif inType == "ini":
            myResponse=parseINI(myResponse)
        elif inType != "csv":   
            myResponse=parseJSON(myResponse)
            myResponse=parseYAML(myResponse)
            myResponse=parseINI(myResponse)
            myResponse=parseXML(myResponse)
    return myResponse

def setProducer(inConfig):
    if inConfig.get('security_protocol') is None:
        print('Setting Brokers: ',inConfig.get('bootstrap_servers'))
        return KafkaProducer(
            bootstrap_servers=inConfig.get('bootstrap_servers')
        )
    elif inConfig.get('security_protocol') == 'ssl':
        return KafkaProducer(
            bootstrap_servers=inConfig.get('bootstrap_servers'),
            security_protocol=inConfig.get('security_protocol'),
            ssl_cafile=inConfig.get('ssl_cafile'),
            ssl_certfile=inConfig.get('ssl_certfile'),
            ssl_keyfile=inConfig.get('ssl_keyfile')
        )
    elif inConfig.get('security_protocol') == 'sasl_plain':
        return KafkaProducer(
            bootstrap_servers=inConfig.get('bootstrap_servers'),
            security_protocol=inConfig.get('security_protocol'),
            sasl_plain_username=inConfig.get('sasl_plain_username'),
            sasl_plain_password=inConfig.get('sasl_plain_password')
        )
    else:
        return None
    
def produceAs(inData,outType = "json"):
    if inData == None:
        return None
    elif outType == None or outType == "txt" or outType == "text":
        return inData
    elif outType == "yaml" or outType == "yml":
        return yaml.safe_dump(inData)
    else:
        return json.dumps(inData)

# Function to replace mustache syntax in string with corresponding data values
def parseMessage(inTemplate, inData):
    if isinstance(inData, dict):
        for key, value in inData.items():
            inTemplate = inTemplate.replace('{{' + key + '}}', value)
    return inTemplate

def encodeValue(inValue = None):
    if inValue == None:
        return None
    elif isinstance(inValue, str):
        return inValue.encode('utf-8')

# Function to produce a message to Kafka
def produceMessage(producer, topic, message, debug=False, partition=-1, key=None, header=None):
    if debug == True: 
        print(f"Topic: {topic}:{partition}\nKey: {key}\nHeader: {header}\nMessage: {message}\n")
    else: 
        try:
            if partition == -1:
                response=producer.send(topic=topic, key=encodeValue(key), headers=[('headers',encodeValue(header))], value=encodeValue(message))
                print(f"Producing message to topic: {topic}")
            else:
                response=producer.send(topic=topic, key=encodeValue(key), partition=partition, headers=[('headers',encodeValue(header))], value=encodeValue(message))
                print(f"Producing message to topic: {topic}, partition: {str(partition)}")
            meta = response.get(timeout=10)
            print("Producing message to topic: ", meta)
        except Exception as e:
            print("Exception while producing record value - {} to topic - {} with key - {} and header - {}. Exception {}".format(message, topic, key, header, e))
            pass

def parseArgs():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Produce messages to a Kafka topic')
    parser.add_argument('--config', type=str, required=True, help='Path to configuration file (JSON or YAML)')
    parser.add_argument('--data', type=str, required=False, help='Data used to replace template')
    parser.add_argument('--dataType', type=str, default="json", help='Data Type of the input data')
    parser.add_argument('--debug', type=bool, default=False, help='Header for the Kafka message')
    parser.add_argument('--header', type=str, default=None, help='Header for the Kafka message')
    parser.add_argument('--headerType', type=str, default="json", help='Type for the message Header')
    parser.add_argument('--key', type=str, default=None, help='Key for the Kafka message')
    parser.add_argument('--keyType', type=str, default="text", help='Type of key to produce')
    parser.add_argument('--multi', type=bool, default=False, help='If enabled sends every line or array value as a message')
    parser.add_argument('--csvHeaders', type=str, default=None, help='CSV Headers to use when parsing CSV files')
    parser.add_argument('--partition', type=int, default=-1, help='Partition where to load the value')
    parser.add_argument('--topic', type=str, required=True, help='Kafka topic')
    parser.add_argument('--type', type=str, default="json", help='Type of message to produce')
    parser.add_argument('--value', type=str, required=True, help='Message payload or Template')
    parser.add_argument('--valueType', type=str, required=False, default="json", help='Type of message payload')
    return parser.parse_args()


def main():
    args = parseArgs()
    config=parseInput(args.config)
    producer=setProducer(config.get('KafkaConnection'))

    # Prepare message data
    value=parseInput(args.value,args.valueType) if args.value else None
    data=parseInput(args.data,args.dataType,args.csvHeaders) if args.data else None
    key=parseInput(args.key,args.keyType) if args.key else None
    header=parseInput(args.header,args.headerType) if args.header else None
    # Produce messages
    if args.multi and isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                _topic = parseMessage(args.topic, item)
                #_partition = parseMessage(args.partition, item)
                _key = parseMessage(produceAs(key, "json"), item) if args.key else None
                _message = parseMessage(produceAs(value, args.type), item)
                _header = parseMessage(produceAs(header, "json"), item) if args.header else None
                produceMessage(producer, _topic, _message, args.debug, args.partition, _key, _header)
    else:
        _topic = parseMessage(args.topic, data)
        #_partition = parseMessage(args.partition, item)
        _key = parseMessage(produceAs(key, "json"), data) if args.key else None
        _message = parseMessage(produceAs(value, args.type), data)
        _header = parseMessage(produceAs(header, "json"), data) if args.header else None
        produceMessage(producer, _topic, _message, args.debug, args.partition, _key, _header)

    # Flush and close the producer
    producer.flush()
    producer.close()

if __name__ == '__main__':
    main()