#!/bin/bash

# Source the configuration script
source kafka_config.sh

# Get today's date
today=$(date +%Y-%m-%d)
today_timestamp=$(date -d "$today" +%s)

# Loop through each topic and consumer group combination
for item in "${topics_and_consumer_groups[@]}"; do
  # Extract the topic and consumer groups from the item
  IFS=":" read -r topic consumer_groups <<< "$item"
  IFS="," read -ra consumer_groups_arr <<< "$consumer_groups"

  # Loop through each consumer group
  for consumer_group in "${consumer_groups_arr[@]}"; do
    # Loop through each day in the range
    start_date=$(date -d "-9 days" +%Y-%m-%d)  # Adjust the number of days as needed
    start_timestamp=$(date -d "$start_date" +%s)

    current_date=$start_timestamp
    end_date=$today_timestamp

    while [ "$current_date" -lt "$end_date" ]; do
      date_string=$(date -d "@$current_date" "+%Y-%m-%d")

      # Convert the date to UNIX timestamp
      timestamp=$(date -d "$date_string" +%s)

      # Get the start and end timestamps for the specified day
      start_timestamp=$timestamp
      end_timestamp=$((timestamp + 86400))  # Add 24 hours in seconds

      # Run the kafka-console-consumer command to consume messages and count them
      command="kafka-console-consumer --bootstrap-server $brokers --topic $topic --consumer.config consumer.properties --group $consumer_group --from-beginning --timeout-ms 500 --property print.timestamp=true --property print.key=false --property print.value=false --property key.separator=, --property value.deserializer=org.apache.kafka.common.serialization.ByteArrayDeserializer | awk -F, -v start=$start_timestamp -v end=$end_timestamp 'BEGIN { count=0 } { if (\$1 >= start && \$1 < end) { count++ } } END { print count }'"

      # Execute the command
      result=$(eval $command)

      # Print the result
      echo "Topic: $topic, Consumer Group: $consumer_group, Date: $date_string, Count: $result"

      # Move to the next day
      current_date=$((current_date + 86400))
    done
  done
done
