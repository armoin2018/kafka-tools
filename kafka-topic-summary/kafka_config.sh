#!/bin/bash

# Define the topics and consumer groups array
declare -a topics_and_consumer_groups=(
  "my-topic1:consumer-group1,consumer-group2"
  "my-topic2:consumer-group3"
)


brokers="kafka1:9092,kafka2:9092"