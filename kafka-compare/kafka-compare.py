import argparse
import asyncio
import difflib
import json
import os
import yaml
import concurrent.futures

from confluent_kafka import Consumer, KafkaException
from bs4 import BeautifulSoup


class MessageCache:
    def __init__(self, cache_folder):
        self.cache_folder = cache_folder
        self.comparisons = []

    def save_message(self, topic, message):
        os.makedirs(os.path.join(self.cache_folder, topic), exist_ok=True)
        file_name = f"{topic}_{message.offset()}.json"
        file_path = os.path.join(self.cache_folder, topic, file_name)
        with open(file_path, "w") as file:
            file.write(json.dumps(message.value()))

    def compare_messages(self, topic):
        files = os.listdir(os.path.join(self.cache_folder, topic))
        files.sort(key=lambda x: int(x.split("_")[1].split(".")[0]))

        previous_message = None
        comparison_results = []
        for file in files:
            file_path = os.path.join(self.cache_folder, topic, file)
            with open(file_path, "r") as file:
                current_message = json.loads(file.read())
            if previous_message:
                diff = difflib.unified_diff(
                    json.dumps(previous_message, sort_keys=True, indent=4).splitlines(),
                    json.dumps(current_message, sort_keys=True, indent=4).splitlines(),
                )
                diff_str = "\n".join(diff)
                comparison_results.append(
                    {"file": file, "diff": diff_str}
                )

            previous_message = current_message

        return topic, comparison_results


class KafkaConsumer:
    def __init__(self, config):
        self.config = config
        self.message_cache = MessageCache(config["cache_folder"])

    def consume_topic(self, topic, kafka_config):
        consumer = Consumer(kafka_config)
        try:
            consumer.subscribe([topic])
            while True:
                message = consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaException._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(message.error())
                self.message_cache.save_message(topic, message)
        finally:
            consumer.close()

    async def compare_topics(self, topics):
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config["comparison_threads"]
        ) as executor:
            loop = asyncio.get_event_loop()
            tasks = [
                loop.run_in_executor(
                    executor, self.compare_topic, topic, self.config["topics"][topic]
                )
                for topic in topics
            ]
            results = await asyncio.gather(*tasks)

        self.message_cache.comparisons = results

    def compare_topic(self, topic, kafka_config):
        self.consume_topic(topic, kafka_config)
        return self.message_cache.compare_messages(topic)


def load_config(config_file):
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config


def generate_txt_report(comparisons):
    with open("comparison_report.txt", "w") as file:
        for topic, results in comparisons:
            file.write(f"Comparison for topic: {topic}\n")
            for result in results:
                file.write(f"--- {result['file']}\n")
                file.write(f"+++ {result['file']}\n")
                file.write(result["diff"])
                file.write("\n\n")


def generate_csv_report(comparisons):
    import csv

    with open("comparison_report.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["Topic", "File", "Diff"])
        for topic, results in comparisons:
            for result in results:
                writer.writerow([topic, result["file"], result["diff"]])


def generate_json_report(comparisons):
    with open("comparison_report.json", "w") as file:
        json.dump(comparisons, file, indent=4)


def generate_html_report(comparisons):
    with open("comparison_report.html", "w") as file:
        soup = BeautifulSoup(features="html.parser")
        html_tag = soup.new_tag("html")
        head_tag = soup.new_tag("head")
        title_tag = soup.new_tag("title")
        title_tag.string = "Comparison Report"
        head_tag.append(title_tag)
        body_tag = soup.new_tag("body")
        heading_tag = soup.new_tag("h1")
        heading_tag.string = "Comparison Report"
        body_tag.append(heading_tag)
        for topic, results in comparisons:
            topic_tag = soup.new_tag("h2")
            topic_tag.string = f"Topic: {topic}"
            body_tag.append(topic_tag)
            for result in results:
                file_tag = soup.new_tag("h3")
                file_tag.string = f"File: {result['file']}"
                body_tag.append(file_tag)
                pre_tag = soup.new_tag("pre")
                pre_tag.string = result["diff"]
                body_tag.append(pre_tag)
        html_tag.append(head_tag)
        html_tag.append(body_tag)
        soup.append(html_tag)
        file.write(soup.prettify())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Topic Comparison Script")
    parser.add_argument(
        "config_file", metavar="config_file", type=str, help="Path to the config file"
    )
    args = parser.parse_args()

    config = load_config(args.config_file)

    consumer = KafkaConsumer(config)

    topics = list(config["topics"].keys())

    asyncio.run(consumer.compare_topics(topics))

    comparisons = consumer.message_cache.comparisons

    output_format = config.get("output_format", "txt")

    if output_format == "txt":
        generate_txt_report(comparisons)
    elif output_format == "csv":
        generate_csv_report(comparisons)
    elif output_format == "json":
        generate_json_report(comparisons)
    elif output_format == "html":
        generate_html_report(comparisons)
    else:
        print("Invalid output format specified in the configuration file.")
