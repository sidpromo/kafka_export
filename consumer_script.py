import json
import configparser
import logging
import logging.handlers
import logging.config
from datetime import datetime
from kafka import KafkaConsumer


config_file_path='config.ini'
# config_file_path='logger_config.ini'
output_file_path='data.json'


def configure_logging():
    log_file_path='kafka_processing.log'
    log_max_bytes = 10485760  # 10MB
    log_backup_count = 5

    log_handler = logging.handlers.RotatingFileHandler(filename=log_file_path, maxBytes=log_max_bytes, backupCount=log_backup_count)
    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.addHandler(log_handler)
    logger.setLevel(logging.INFO)


def load_logging_config():
    config = configparser.ConfigParser()
    config.read(config_file_path)
    logging_config = config['LOGGING']

    # Load the logging configuration from the config file
    logging.config.dictConfig(logging_config)


def is_json(obj):
    try:
        json_obj=json.loads(obj)
        return True
    except ValueError:
        return False


def save_last_run(config, date):
    config['DEFAULT']={'timestamp':str(date)}

    with open(config_file_path, 'w') as config_file:
        config.write(config_file)


def load_last_run(config):
    ts=float(config['DEFAULT']['timestamp'])


def read_json_from_kafka(topic, start_time, config, bootstrap_servers='localhost:9092'):
    # consumer = KafkaConsumer(topic,bootstrap_servers=bootstrap_servers, value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset='earliest')
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers,auto_offset_reset='earliest', session_timeout_ms=6000)
    print("Consumer created")
    print(consumer.topics())
    #consumer.subscribe(topic)

    end_time=datetime.now()
    start_timestamp = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_timestamp = end_time

    for message in consumer:
        message_time = datetime.fromtimestamp(message.timestamp / 1000.0)

        if message_time >= start_timestamp and message_time <= end_timestamp:
            print(f"Received message at {message_time}: {message.value}")
            # if is_json(message.value):
            #     print(f"Received json message at {message_time}: {message.value}")

        if message_time > end_timestamp:
            print("Message ts too late ")
            break

    consumer.close()
    save_last_run(config, end_time)


def poll_version(topic, start_time, config, bootstrap_servers='localhost:9092'):
    consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, auto_offset_reset='earliest')
    print(consumer.topics())

    # last_run_time = load_last_run()
    # if last_run_time:
    #     start_timestamp = last_run_time

    start_timestamp = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
    end_timestamp = datetime.now()

    # while start_timestamp <= end_timestamp:
    # Fetch messages using poll()
    with open(output_file_path,'a') as file:
        messages = consumer.poll(timeout_ms=6000)  # Adjust the timeout as needed
        invalid_event_count = valid_event_count = 0

        for topic_partition, records in messages.items():
            for record in records:
                message_time = datetime.fromtimestamp(record.timestamp / 1000.0)

                if start_timestamp <= message_time <= end_timestamp:
                    if is_json(record.value):
                        json_data = json.loads(record.value)
                        json.dump(json_data, file)
                        file.write('\n')
                        valid_event_count+=1
                        print(f"Received json message at {message_time}: {record.value}")
                    else:
                        invalid_event_count+=1

    # start_timestamp = datetime.now()

    print(f'Invalid event count: {invalid_event_count} | valid event count" {valid_event_count}')
    consumer.close()
    save_last_run(config, end_timestamp)


if __name__ == '__main__':
    # Usage example
    topic = 'bgf_ebm-processed'
    start_time = '2020-05-01 00:00:00'
    bootstrap_servers = 'seliics00379e02.seli.gic.ericsson.se:9092'
    configure_logging()
    # load_logging_config()
    logger = logging.getLogger()

    config=configparser.ConfigParser()
    config.read(config_file_path)

    # read_json_from_kafka(topic, start_time, bootstrap_servers)
    try:
        poll_version(topic, start_time, config, bootstrap_servers)
    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
